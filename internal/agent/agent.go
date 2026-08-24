package agent

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/makinje/aero-arc-agent/internal/identity"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type Agent struct {
	node *gomavlib.Node
	wal  *wal.WAL

	conn    *grpc.ClientConn
	gateway agentv1.AgentGatewayClient

	options *AgentOptions

	// goroutine waitgroup
	wg sync.WaitGroup

	// reconnection/backoff settings – wired from AgentOptions.
	backoffInitial time.Duration
	backoffMax     time.Duration

	// Internal hooks primarily for testing; in production these are wired to
	// the concrete implementations below.
	dialFn         func(ctx context.Context) (*grpc.ClientConn, error)
	registerFn     func(ctx context.Context) error
	openStreamFn   func(ctx context.Context) (grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], error)
	ackLoopFn      func(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage]) error
	sleepWithBack  func(ctx context.Context, d time.Duration) bool
	closeWALFn     func(ctx context.Context) error
	closeMAVLinkFn func(ctx context.Context)

	stateMu          sync.RWMutex
	sessionID        string
	operationContext *wal.OperationContext
	sendMu           sync.Mutex

	mavlinkMu             sync.Mutex
	mavlinkTarget         *mavlinkTarget
	mavlinkHeartbeatSeq   uint64
	pendingMAVLinkCommand *pendingMAVLinkCommand
	aircraftAckAmbiguous  bool
	aircraftCommandMu     sync.Mutex
	aircraftCommandActive bool
	writeMAVLinkCommand   func(*gomavlib.Channel, *common.MessageCommandLong) error

	ingestCount atomic.Uint64
	sendCount   atomic.Uint64
}

// NewAgent constructs an Agent and its MAVLink endpoint from runtime options.
// It also installs the default Relay connection and stream lifecycle hooks;
// durable WAL resources are opened later by Start.
//
// Parameters:
//   - options: provides the configuration values used to initialize or execute the operation.
//
// Returns:
//   - agent: is configured but does not validate/open the MAVLink endpoint,
//     network connection, or WAL until Start.
//   - error: is currently always nil; deferred resource failures surface from Start.
func NewAgent(options *AgentOptions) (*Agent, error) {
	if options.BackoffInitial <= 0 {
		options.BackoffInitial = time.Second
	}
	if options.BackoffMax <= 0 {
		options.BackoffMax = 30 * time.Second
	}

	a := &Agent{
		node: &gomavlib.Node{
			Endpoints: []gomavlib.EndpointConf{
				gomavlib.EndpointSerial{
					Device: options.SerialPath,
					Baud:   options.SerialBaud,
				},
			},
			OutVersion:     gomavlib.V2,
			OutSystemID:    mavlinkSourceSystemID,
			OutComponentID: mavlinkSourceComponentID,
			Dialect:        common.Dialect,
		},
		options:        options,
		backoffInitial: options.BackoffInitial,
		backoffMax:     options.BackoffMax,
		// A COMMAND_ACK buffered before a process restart is indistinguishable
		// from the first new ARM/DISARM acknowledgement. Start fenced and use
		// fresh post-send heartbeat state as the durable command boundary.
		aircraftAckAmbiguous: true,
	}
	a.writeMAVLinkCommand = func(channel *gomavlib.Channel, command *common.MessageCommandLong) error {
		return a.node.WriteMessageTo(channel, command)
	}

	if options.Debug {
		slog.LogAttrs(context.Background(), slog.LevelInfo, "debug mode enabled, using UDP mavlinkserver")
		a.node = &gomavlib.Node{
			Endpoints: []gomavlib.EndpointConf{
				gomavlib.EndpointUDPServer{
					Address: "0.0.0.0:14550",
				},
			},
			OutVersion:     gomavlib.V2,
			OutSystemID:    mavlinkSourceSystemID,
			OutComponentID: mavlinkSourceComponentID,
			Dialect:        common.Dialect,
		}
	}

	// Wire default implementations for lifecycle hooks.
	a.dialFn = a.establishRelayConnection
	a.registerFn = a.register
	a.openStreamFn = a.openTelemetryStream
	a.ackLoopFn = a.runAckLoop
	a.sleepWithBack = sleepWithContext

	return a, nil
}

// Start runs the MAVLink ingest loop and the gRPC reconnect/stream lifecycle
// until the provided context is cancelled or a fatal error occurs.
func (a *Agent) Start(ctx context.Context) (startErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Ensure resources are cleaned up on exit.
	defer func() {
		// Use a fresh context for shutdown since 'ctx' might be cancelled.
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelShutdown()
		if err := a.shutdown(shutdownCtx); err != nil {
			startErr = errors.Join(startErr, fmt.Errorf("agent shutdown: %w", err))
		}
	}()

	// Resolve Identity
	identity := identity.Resolve()
	slog.LogAttrs(ctx, slog.LevelInfo, "agent_identity", slog.String("identity", identity.FinalID))

	// Initialize WAL
	w, err := wal.New(ctx, a.options.WALPath, a.options.WALBatchSize, a.options.WALFlushTimeout)
	if err != nil {
		return fmt.Errorf("failed to initialize WAL: %w", err)
	}
	a.wal = w
	if operationContext, ok, err := w.LoadOperationContext(ctx); err != nil {
		return err
	} else if ok {
		a.operationContext = &operationContext
	}
	slog.LogAttrs(ctx, slog.LevelInfo, "wal_initialized", slog.String("path", a.options.WALPath))

	a.wg.Add(1)
	go func(ctx context.Context) {
		defer a.wg.Done()
		a.runTelemetryStats(ctx, 10*time.Second)
	}(ctx)

	// Run WAL cleanup loop
	a.wg.Add(1)
	go func(ctx context.Context) {
		defer a.wg.Done()
		timer := time.NewTicker(10 * time.Second)
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				// Cleanup delivered frames
				if err := a.wal.CleanupDelivered(ctx, 10000); err != nil {
					slog.LogAttrs(ctx, slog.LevelError, "wal_cleanup_failed", slog.String("error", err.Error()))
				}

				// Reset stuck pending frames (e.g. 5 minute TTL)
				// This handles cases where a frame was marked pending but we never got an ACK or crashed.
				if _, err := a.wal.ResetPending(ctx, 5*time.Minute); err != nil {
					slog.LogAttrs(ctx, slog.LevelError, "wal_reset_pending_failed", slog.String("error", err.Error()))
				}
			}
		}
	}(ctx)

	// Run MAVLink loop
	a.wg.Add(1)
	go func(ctx context.Context) {
		defer a.wg.Done()
		a.runMAVLink(ctx)
	}(ctx)

	a.wg.Add(1)
	go func(ctx context.Context) {
		defer a.wg.Done()
		a.runWithReconnect(ctx)
	}(ctx)

	<-ctx.Done()

	slog.LogAttrs(ctx, slog.LevelInfo, "agent received shutdown signal", slog.String("signal", ctx.Err().Error()))

	return ctx.Err()
}

func (a *Agent) shutdown(ctx context.Context) error {
	var shutdownErr error
	if a.conn != nil {
		slog.Info("shutting down grpc connection")
		if err := a.conn.Close(); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("close gRPC connection: %w", err))
		}
	}

	if a.closeWALFn != nil {
		slog.Info("shutting down write-ahead log connection")
		if err := a.closeWALFn(ctx); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("close write-ahead log: %w", err))
		}
	} else if a.wal != nil {
		slog.Info("shutting down write-ahead log connection")
		if err := a.wal.CloseContext(ctx); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("close write-ahead log: %w", err))
		}
	}

	slog.Info("shutting down mavlink node (best effort)")
	mavlinkCloseCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	if a.closeMAVLinkFn != nil {
		a.closeMAVLinkFn(mavlinkCloseCtx)
	} else {
		a.closeMAVLinkBestEffort(mavlinkCloseCtx)
	}

	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		slog.Warn("shutdown timed out waiting for goroutines to finish")
		shutdownErr = errors.Join(shutdownErr, ctx.Err())
	}

	a.gateway = nil
	a.conn = nil

	return shutdownErr
}

func (a *Agent) closeMAVLinkBestEffort(ctx context.Context) {
	if a.node == nil {
		slog.Info("mavlink node already shutdown or closed")
		return
	}

	done := make(chan struct{})
	go func() {
		a.node.Close()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("mavlink node closed successfully")
	case <-ctx.Done():
		slog.Warn("mavlink node close timed out; continuing shutdown")
	}
}

// runMAVLink owns the lifecycle of the gomavlib node.
func (a *Agent) runMAVLink(ctx context.Context) error {
	slog.LogAttrs(ctx, slog.LevelInfo, "mavlink_node_initializing")

	if err := a.node.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize node: %v", err)
	}

	slog.LogAttrs(ctx, slog.LevelInfo, "mavlink_node_initialized")
	events := a.node.Events()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt, ok := <-events:
			if !ok {
				slog.LogAttrs(ctx, slog.LevelInfo, "mavlink eventstream closed")
				return nil
			}

			if frameEvt, ok := evt.(*gomavlib.EventFrame); ok {
				a.observeMAVLinkFrame(frameEvt)
				slog.LogAttrs(
					ctx, slog.LevelDebug,
					"mavlink_frame_received",
					slog.String("frame-message", fmt.Sprintf("%+v", frameEvt.Message())),
				)

				// Process frame asynchronously via WAL batcher
				if err := a.processFrame(ctx, frameEvt); err != nil {
					slog.LogAttrs(
						ctx, slog.LevelError,
						"failed_to_process_frame",
						slog.String("error", err.Error()),
					)
					continue
				}
			}

			if _, ok := evt.(*gomavlib.EventChannelOpen); ok {
				slog.LogAttrs(
					ctx, slog.LevelInfo,
					"mavlink_channel_open",
					slog.String("relay-address", a.options.ServerAddress),
					slog.Int("relay-port", a.options.ServerPort),
				)
				continue
			}

			if closeEvent, ok := evt.(*gomavlib.EventChannelClose); ok {
				a.clearMAVLinkTarget(closeEvent.Channel)
				slog.LogAttrs(
					ctx, slog.LevelInfo,
					"mavlink_channel_close",
					slog.String("relay-address", a.options.ServerAddress),
					slog.Int("relay-port", a.options.ServerPort),
				)
				continue
			}
		}
	}
}

// processFrame marshals the MAVLink frame and queues it for WAL ingestion.
func (a *Agent) processFrame(ctx context.Context, frame *gomavlib.EventFrame) error {
	msg := frame.Message()
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal frame message: %w", err)
	}

	msgName := fmt.Sprintf("%T", msg)
	fields, _ := commonFields(msg)

	// Construct the TelemetryFrame to return
	tFrame := &agentv1.TelemetryFrame{
		RawMavlink:   payload,
		SentAtUnixNs: time.Now().UnixNano(),
		Dialect:      "common",
		MsgId:        msg.GetID(),
		MsgName:      msgName,
		Fields:       fields,
		AgentId:      identity.Resolve().FinalID,
	}
	a.stampFrameContext(tFrame)

	// Write to WAL asynchronously
	if err := a.wal.AppendAsync(ctx, tFrame); err != nil {
		return fmt.Errorf("wal append async failed: %w", err)
	}
	a.ingestCount.Add(1)

	return nil
}

func (a *Agent) stampFrameContext(frame *agentv1.TelemetryFrame) {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if active := a.operationContext; active != nil {
		frame.FlightId = active.FlightID
		frame.IntentId = active.IntentID
		frame.IntentVersion = active.IntentVersion
	}
}

func (a *Agent) stampCurrentSession(frame *agentv1.TelemetryFrame) {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	frame.SessionId = a.sessionID
}

// dialRelay establishes a gRPC connection to the relay using the configured target.
func (a *Agent) establishRelayConnection(ctx context.Context) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	creds, err := relayTransportCredentials(a.options)
	if err != nil {
		if errors.Is(err, ErrGettingHomeDir) {
			slog.LogAttrs(ctx, slog.LevelError, ErrGettingHomeDir.Error(), slog.String("error", err.Error()))
		}
		return nil, err
	}

	slog.LogAttrs(
		dialCtx, slog.LevelInfo,
		"agent_connecting",
		slog.String("target", a.options.RelayTarget),
	)

	conn, err := grpc.NewClient(
		a.options.RelayTarget,
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(TokenAuth{
			Token:  a.options.APIKey,
			Secure: !a.options.Debug,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFailedToConnectToServer, err)
	}

	return conn, nil
}

func relayTransportCredentials(options *AgentOptions) (credentials.TransportCredentials, error) {
	if options.SkipTLSVerification {
		return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}), nil //nolint:gosec // Explicit development-only CLI option.
	}
	if options.Debug {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrGettingHomeDir, err)
		}
		certPath := fmt.Sprintf("%s/%s", homeDir, DebugTLSCertPath)
		creds, err := credentials.NewClientTLSFromFile(certPath, "localhost")
		if err != nil {
			return nil, err
		}
		return creds, nil
	}
	return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}), nil
}

// register performs the Register RPC with the relay.
func (a *Agent) register(ctx context.Context) error {
	if a.gateway == nil {
		return ErrGatewayNotInitialized
	}

	agentID := identity.Resolve().FinalID
	req := &agentv1.RegisterRequest{
		AgentId: agentID,
	}

	slog.LogAttrs(
		ctx, slog.LevelInfo,
		"agent_registering",
		slog.String("target", a.options.RelayTarget),
	)

	regCtx := metadata.AppendToOutgoingContext(ctx, "aero-arc-agent-id", agentID)
	response, err := a.gateway.Register(regCtx, req)
	if err != nil {
		return err
	}
	if response.GetSessionId() == "" {
		return errors.New("relay registration returned an empty session ID")
	}
	a.stateMu.Lock()
	a.sessionID = response.GetSessionId()
	a.stateMu.Unlock()

	slog.LogAttrs(
		ctx, slog.LevelInfo,
		"agent_registered",
		slog.String("target", a.options.RelayTarget),
	)

	return nil
}

// openTelemetryStream opens the bidi telemetry stream.
func (a *Agent) openTelemetryStream(ctx context.Context) (grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], error) {
	if a.gateway == nil {
		return nil, ErrGatewayNotInitialized
	}

	slog.LogAttrs(
		ctx, slog.LevelInfo,
		"agent_stream_opening",
		slog.String("target", a.options.RelayTarget),
	)

	agentID := identity.Resolve().FinalID
	a.stateMu.RLock()
	sessionID := a.sessionID
	a.stateMu.RUnlock()
	if sessionID == "" {
		return nil, errors.New("relay session ID is unavailable")
	}
	streamCtx := metadata.AppendToOutgoingContext(ctx,
		"aero-arc-agent-id", agentID,
		"aero-arc-session-id", sessionID,
	)

	stream, err := a.gateway.TelemetryStream(streamCtx)
	if err != nil {
		return nil, err
	}

	slog.LogAttrs(
		ctx, slog.LevelInfo,
		"agent_stream_open",
		slog.String("target", a.options.RelayTarget),
	)

	return stream, nil
}

// runStreamLoop handles the receive side of the telemetry stream. Outbound
// sends will be wired in a later iteration once the queue is implemented.
func (a *Agent) runAckLoop(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage]) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			message, err := stream.Recv()
			if err != nil {
				return err
			}

			err = a.handleRelayMessage(ctx, stream, message)
			if err != nil {
				// TODO: Handle error? Should we retry? Definitely shouldn't just exit.
				return err
			}
		}
	}
}

func (a *Agent) handleRelayMessage(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], message *agentv1.RelayStreamMessage) error {
	switch payload := message.GetPayload().(type) {
	case *agentv1.RelayStreamMessage_TelemetryAck:
		return a.handleTelemetryAck(ctx, payload.TelemetryAck)
	case *agentv1.RelayStreamMessage_SetOperationContext:
		return a.handleSetOperationContext(ctx, stream, payload.SetOperationContext)
	case *agentv1.RelayStreamMessage_ClearOperationContext:
		return a.handleClearOperationContext(ctx, stream, payload.ClearOperationContext)
	case *agentv1.RelayStreamMessage_AircraftCommand:
		return a.handleAircraftCommand(ctx, stream, payload.AircraftCommand)
	default:
		return a.sendOperationContextAck(stream, "", agentv1.OperationContextCommandAck_STATUS_REJECTED, "relay stream message has no supported payload")
	}
}

func (a *Agent) handleSetOperationContext(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], command *agentv1.SetOperationContextCommand) error {
	if command.GetCommandId() == "" || command.GetContext() == nil || command.GetContext().GetFlightId() == "" {
		return a.sendOperationContextAck(stream, command.GetCommandId(), agentv1.OperationContextCommandAck_STATUS_REJECTED, "command_id and flight_id are required")
	}
	value := wal.OperationContext{FlightID: command.Context.FlightId, IntentID: command.Context.IntentId, IntentVersion: command.Context.IntentVersion}
	applied, err := a.wal.SetOperationContext(ctx, command.CommandId, value)
	if err != nil {
		if errors.Is(err, wal.ErrOperationCommandConflict) {
			return a.sendOperationContextAck(stream, command.CommandId, agentv1.OperationContextCommandAck_STATUS_REJECTED, err.Error())
		}
		return a.sendOperationContextAck(stream, command.CommandId, agentv1.OperationContextCommandAck_STATUS_TEMPORARY_ERROR, err.Error())
	}
	status := agentv1.OperationContextCommandAck_STATUS_APPLIED
	if !applied {
		status = agentv1.OperationContextCommandAck_STATUS_ALREADY_APPLIED
		persisted, ok, loadErr := a.wal.LoadOperationContext(ctx)
		if loadErr != nil {
			return loadErr
		}
		a.stateMu.Lock()
		if ok {
			a.operationContext = &persisted
		} else {
			a.operationContext = nil
		}
		a.stateMu.Unlock()
		return a.sendOperationContextAck(stream, command.CommandId, status, "")
	}
	a.stateMu.Lock()
	a.operationContext = &value
	a.stateMu.Unlock()
	return a.sendOperationContextAck(stream, command.CommandId, status, "")
}

func (a *Agent) handleClearOperationContext(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], command *agentv1.ClearOperationContextCommand) error {
	if command.GetCommandId() == "" {
		return a.sendOperationContextAck(stream, "", agentv1.OperationContextCommandAck_STATUS_REJECTED, "command_id is required")
	}
	if command.GetFlightId() == "" {
		return a.sendOperationContextAck(stream, command.GetCommandId(), agentv1.OperationContextCommandAck_STATUS_REJECTED, "flight id is required")
	}
	applied, err := a.wal.ClearOperationContext(ctx, command.CommandId, command.FlightId)
	if err != nil {
		if errors.Is(err, wal.ErrOperationCommandConflict) {
			return a.sendOperationContextAck(stream, command.CommandId, agentv1.OperationContextCommandAck_STATUS_REJECTED, err.Error())
		}
		return a.sendOperationContextAck(stream, command.CommandId, agentv1.OperationContextCommandAck_STATUS_TEMPORARY_ERROR, err.Error())
	}
	active, ok, err := a.wal.LoadOperationContext(ctx)
	if err != nil {
		return err
	}
	a.stateMu.Lock()
	if ok {
		a.operationContext = &active
	} else {
		a.operationContext = nil
	}
	a.stateMu.Unlock()
	status := agentv1.OperationContextCommandAck_STATUS_APPLIED
	if !applied {
		status = agentv1.OperationContextCommandAck_STATUS_ALREADY_APPLIED
	}
	return a.sendOperationContextAck(stream, command.CommandId, status, "")
}

func (a *Agent) sendOperationContextAck(stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], commandID string, status agentv1.OperationContextCommandAck_Status, errorMessage string) error {
	a.stateMu.RLock()
	var active *agentv1.OperationContext
	if value := a.operationContext; value != nil {
		active = &agentv1.OperationContext{FlightId: value.FlightID, IntentId: value.IntentID, IntentVersion: value.IntentVersion}
	}
	a.stateMu.RUnlock()
	message := &agentv1.AgentStreamMessage{Payload: &agentv1.AgentStreamMessage_OperationContextCommandAck{OperationContextCommandAck: &agentv1.OperationContextCommandAck{CommandId: commandID, Status: status, Error: errorMessage, ActiveContext: active}}}
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	return stream.Send(message)
}

func (a *Agent) handleTelemetryAck(ctx context.Context, ack *agentv1.TelemetryAck) error {
	slog.LogAttrs(
		ctx, slog.LevelDebug,
		"telemetry_ack_received",
		slog.String("ack", fmt.Sprintf("%+v", ack)),
	)

	if _, err := a.wal.MarkDelivered(ctx, ack.Seq); err != nil {
		return fmt.Errorf("failed to mark telemetry ack as delivered: %w", err)
	}

	return nil
}

func (a *Agent) handleTelemetryFrames(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage]) error {
	// The new architecture unifies "Replay" and "Live" into a single loop.
	// 1. We poll the WAL for undelivered frames.
	// 2. We send them.
	// 3. If there are no frames, we wait for a signal from the WAL writer (WaitForData).

	slog.LogAttrs(ctx, slog.LevelInfo, "telemetry_stream_sender_starting")

	for {
		// 1. Read undelivered frames
		entries, err := a.wal.ReadUndelivered(ctx, int(a.options.WALBatchSize))
		if err != nil {
			slog.LogAttrs(ctx, slog.LevelError, "wal_read_error", slog.String("error", err.Error()))
			return err
		}

		entriesLen := len(entries)

		if entriesLen == 0 {
			if err := a.wal.WaitForData(ctx); err != nil {
				return err
			}
			continue
		}

		ids := []uint64{}

		// 2. If data exists, send it
		for i := 0; i < entriesLen; i++ {
			tFrame := &agentv1.TelemetryFrame{}
			if err := proto.Unmarshal(entries[i].Payload, tFrame); err != nil {
				slog.LogAttrs(ctx, slog.LevelError, "wal_frame_unmarshal_error", slog.String("error", err.Error()))
				continue
			}
			tFrame.Seq = uint64(entries[i].ID)
			a.stampWALGeneration(tFrame)
			a.stampCurrentSession(tFrame)

			message := &agentv1.AgentStreamMessage{Payload: &agentv1.AgentStreamMessage_TelemetryFrame{TelemetryFrame: tFrame}}
			a.sendMu.Lock()
			err := stream.Send(message)
			a.sendMu.Unlock()
			if err != nil {
				slog.LogAttrs(ctx, slog.LevelError, "telemetry_frame_send_error", slog.String("error", err.Error()))
				break
			}
			a.sendCount.Add(1)

			ids = append(ids, tFrame.Seq)
		}

		if _, err := a.wal.MarkPendingBatch(ctx, ids); err != nil {
			slog.LogAttrs(ctx, slog.LevelError, "wal_mark_pending_batch_error", slog.String("error", err.Error()))
			continue
		}

		slog.LogAttrs(ctx, slog.LevelInfo, "mark_batch_succeed", slog.Int("batch_size", entriesLen))
	}
}

func (a *Agent) stampWALGeneration(frame *agentv1.TelemetryFrame) {
	if frame.WalId == "" {
		frame.WalId = a.wal.GenerationID()
	}
}

// runWithReconnect orchestrates dial → register → stream with exponential
// backoff and context-aware cancellation. It owns the full lifecycle of the
// gRPC connection and telemetry stream.
func (a *Agent) runWithReconnect(ctx context.Context) error {
	backoff := a.backoffInitial
	if backoff <= 0 {
		backoff = time.Second
	}
	maxBackoff := a.backoffMax
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// 1. Establish connection.
		conn, err := a.dialFn(ctx)
		if err != nil {
			slog.LogAttrs(
				ctx, slog.LevelError,
				"agent_connect_failed",
				slog.String("target", a.options.RelayTarget),
				slog.String("error", err.Error()),
				slog.Int64("backoff_ms", backoff.Milliseconds()),
			)

			if !a.sleepWithBack(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		connCtx, cancelConn := context.WithCancel(ctx)

		a.conn = conn
		a.gateway = agentv1.NewAgentGatewayClient(conn)

		// 2. Register with the relay.
		regCtx, cancelReg := context.WithTimeout(ctx, 10*time.Second)
		err = a.registerFn(regCtx)
		cancelReg()
		if err != nil {
			slog.LogAttrs(
				ctx, slog.LevelError,
				"agent_register_failed",
				slog.String("target", a.options.RelayTarget),
				slog.String("error", err.Error()),
				slog.Int64("backoff_ms", backoff.Milliseconds()),
			)

			cancelConn()
			_ = conn.Close()
			a.conn = nil
			a.gateway = nil

			if !a.sleepWithBack(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		// 3. Open telemetry stream.
		stream, err := a.openStreamFn(ctx)
		if err != nil {
			slog.LogAttrs(
				ctx, slog.LevelError,
				"agent_stream_open_failed",
				slog.String("target", a.options.RelayTarget),
				slog.String("error", err.Error()),
				slog.Int64("backoff_ms", backoff.Milliseconds()),
			)

			cancelConn()
			_ = conn.Close()
			a.conn = nil
			a.gateway = nil

			if !a.sleepWithBack(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		errChan := make(chan error, 2)

		// 4. Handle telemetry frames.
		go func() {
			errChan <- a.handleTelemetryFrames(connCtx, stream)
		}()

		// 5. Run the ack loop until it ends or context is cancelled.
		go func() {
			errChan <- a.ackLoopFn(connCtx, stream)
		}()

		select {
		case <-ctx.Done():
			err = ctx.Err()
		case err = <-errChan:
			slog.LogAttrs(ctx, slog.LevelInfo, "stream_ended", slog.String("error", fmt.Sprint(err)))
		}

		slog.LogAttrs(
			ctx, slog.LevelInfo,
			"agent_stream_closed",
			slog.String("target", a.options.RelayTarget),
			slog.String("error", fmt.Sprintf("%v", err)),
		)

		// Cleanup and Reconnect
		cancelConn()
		_ = stream.CloseSend()
		_ = conn.Close()
		a.conn = nil
		a.gateway = nil

		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Reset backoff after a successful connection cycle (even if the
		// stream eventually ended with an error).
		backoff = a.backoffInitial
		if backoff <= 0 {
			backoff = time.Second
		}

		if err != nil {
			slog.LogAttrs(
				ctx, slog.LevelError,
				"agent_stream_error",
				slog.String("target", a.options.RelayTarget),
				slog.String("error", err.Error()),
				slog.Int64("backoff_ms", backoff.Milliseconds()),
			)

			if !a.sleepWithBack(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}
