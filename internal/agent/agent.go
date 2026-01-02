package agent

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/makinje/aero-arc-agent/internal/identity"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type Agent struct {
	node *gomavlib.Node
	wal  *wal.WAL

	conn    *grpc.ClientConn
	gateway agentv1.AgentGatewayClient

	options *AgentOptions

	// reconnection/backoff settings – wired from AgentOptions.
	backoffInitial time.Duration
	backoffMax     time.Duration

	// Internal hooks primarily for testing; in production these are wired to
	// the concrete implementations below.
	dialFn        func(ctx context.Context) (*grpc.ClientConn, error)
	registerFn    func(ctx context.Context) error
	openStreamFn  func(ctx context.Context) (grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck], error)
	ackLoopFn     func(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck]) error
	sleepWithBack func(ctx context.Context, d time.Duration) bool

	ingestCount atomic.Uint64
	sendCount   atomic.Uint64
}

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
			Dialect: common.Dialect,
		},
		options:        options,
		backoffInitial: options.BackoffInitial,
		backoffMax:     options.BackoffMax,
	}

	if options.Debug {
		slog.LogAttrs(context.Background(), slog.LevelInfo, "debug mode enabled, using UDP mavlinkserver")
		a.node = &gomavlib.Node{
			Endpoints: []gomavlib.EndpointConf{
				gomavlib.EndpointUDPServer{
					Address: "0.0.0.0:14550",
				},
			},
			OutVersion:  gomavlib.V2,
			OutSystemID: 1,
			Dialect:     common.Dialect,
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
func (a *Agent) Start(ctx context.Context, sig <-chan os.Signal) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Ensure resources are cleaned up on exit.
	defer func() {
		// Use a fresh context for shutdown since 'ctx' might be cancelled.
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelShutdown()
		// We ignore the error here as we are shutting down anyway.
		_ = a.shutdown(shutdownCtx)
	}()

	// Resolve Identity
	identity := identity.Resolve()
	slog.LogAttrs(ctx, slog.LevelInfo, "agent_identity", slog.String("identity", identity.FinalID))

	// Initialize WAL
	w, err := wal.New(a.options.WALPath, a.options.WALBatchSize, a.options.WALFlushTimeout)
	if err != nil {
		return fmt.Errorf("failed to initialize WAL: %w", err)
	}
	a.wal = w
	slog.LogAttrs(ctx, slog.LevelInfo, "wal_initialized", slog.String("path", a.options.WALPath))

	go a.runTelemetryStats(ctx, 10*time.Second)

	// Run WAL cleanup loop
	go func() {
		timer := time.NewTicker(10 * time.Second)

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
	}()

	// Run MAVLink loop
	go func() {
		a.runMAVLink(ctx)
	}()

	go func() {
		a.runWithReconnect(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sig:
			slog.LogAttrs(ctx, slog.LevelInfo, "agent received shutdown signal", slog.String("signal", fmt.Sprintf("%v", sig)))
			cancel() // Signal all loops to stop
			return nil
		}
	}
}

func (a *Agent) shutdown(ctx context.Context) error {
	if a.conn != nil {
		a.conn.Close()
	}

	a.node.Close()
	if a.wal != nil {
		a.wal.Close()
	}

	a.gateway = nil
	a.conn = nil
	// TODO: Close any other resources that need to be closed.
	// specifically, the bidirectional telemetry stream.
	return nil
}

// runMAVLink owns the lifecycle of the gomavlib node.
func (a *Agent) runMAVLink(ctx context.Context) error {
	slog.LogAttrs(ctx, slog.LevelInfo, "mavlink_node_initializing")

	if err := a.node.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize node: %v", err)
	}

	slog.LogAttrs(ctx, slog.LevelInfo, "mavlink_node_initialized")

	for evt := range a.node.Events() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if frameEvt, ok := evt.(*gomavlib.EventFrame); ok {
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

			if _, ok := evt.(*gomavlib.EventChannelClose); ok {
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

	return nil
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

	// Write to WAL asynchronously
	if err := a.wal.AppendAsync(ctx, tFrame); err != nil {
		return fmt.Errorf("wal append async failed: %w", err)
	}
	a.ingestCount.Add(1)

	return nil
}

// dialRelay establishes a gRPC connection to the relay using the configured target.
func (a *Agent) establishRelayConnection(ctx context.Context) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// TODO: Use a proper TLS config with a valid certificate.
	var creds credentials.TransportCredentials

	if a.options.Debug {
		creds = insecure.NewCredentials()
	} else {
		creds = credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: a.options.SkipTLSVerification,
		})
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
	_, err := a.gateway.Register(regCtx, req)
	if err != nil {
		return err
	}

	slog.LogAttrs(
		ctx, slog.LevelInfo,
		"agent_registered",
		slog.String("target", a.options.RelayTarget),
	)

	return nil
}

// openTelemetryStream opens the bidi telemetry stream.
func (a *Agent) openTelemetryStream(ctx context.Context) (grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck], error) {
	if a.gateway == nil {
		return nil, ErrGatewayNotInitialized
	}

	slog.LogAttrs(
		ctx, slog.LevelInfo,
		"agent_stream_opening",
		slog.String("target", a.options.RelayTarget),
	)

	agentID := identity.Resolve().FinalID
	streamCtx := metadata.AppendToOutgoingContext(ctx, "aero-arc-agent-id", agentID)

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
func (a *Agent) runAckLoop(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck]) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			ack, err := stream.Recv()
			if err != nil {
				return err
			}

			err = a.handleTelemetryAck(ctx, ack)
			if err != nil {
				// TODO: Handle error? Should we retry? Definitely shouldn't just exit.
				return err
			}
		}
	}
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

func (a *Agent) handleTelemetryFrames(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck]) error {
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

			if err := stream.Send(tFrame); err != nil {
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
		defer cancelConn()

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
		case err := <-errChan:
			slog.LogAttrs(ctx, slog.LevelInfo, "stream_ended", slog.String("error", fmt.Sprint(err)))
		}

		cancelConn()

		slog.LogAttrs(
			ctx, slog.LevelInfo,
			"agent_stream_closed",
			slog.String("target", a.options.RelayTarget),
			slog.String("error", fmt.Sprintf("%v", err)),
		)

		// Cleanup and Reconnect
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
