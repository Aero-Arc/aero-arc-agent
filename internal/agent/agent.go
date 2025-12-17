package agent

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/makinje/aero-arc-agent/internal/identity"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type Agent struct {
	node *gomavlib.Node
	wal  *wal.WAL

	conn    *grpc.ClientConn
	gateway agentv1.AgentGatewayClient

	options *AgentOptions

	// reconnection/backoff settings – wired from AgentOptions.
	backoffInitial  time.Duration
	backoffMax      time.Duration
	eventFrameQueue chan *QueuedFrame

	// Internal hooks primarily for testing; in production these are wired to
	// the concrete implementations below.
	dialFn        func(ctx context.Context) (*grpc.ClientConn, error)
	registerFn    func(ctx context.Context) error
	openStreamFn  func(ctx context.Context) (grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck], error)
	ackLoopFn     func(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck]) error
	sleepWithBack func(ctx context.Context, d time.Duration) bool
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
		options:         options,
		backoffInitial:  options.BackoffInitial,
		backoffMax:      options.BackoffMax,
		eventFrameQueue: make(chan *QueuedFrame, options.EventQueueSize),
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

	// Resolve Identity
	identity := identity.Resolve()
	slog.LogAttrs(ctx, slog.LevelInfo, "agent_identity", slog.String("identity", identity.FinalID))

	// Initialize WAL
	if a.options.WALPath != "" {
		w, err := wal.New(a.options.WALPath)
		if err != nil {
			return fmt.Errorf("failed to initialize WAL: %w", err)
		}
		a.wal = w
		slog.LogAttrs(ctx, slog.LevelInfo, "wal_initialized", slog.String("path", a.options.WALPath))

		// Run minimal cleanup on startup to keep size bounded.
		// Retain last 10,000 delivered frames for debugging/audit.
		if err := w.CleanupDelivered(ctx, 10000); err != nil {
			slog.LogAttrs(ctx, slog.LevelError, "wal_cleanup_failed", slog.String("error", err.Error()))
			// Non-fatal, continue startup.
		}
	} else {
		slog.LogAttrs(ctx, slog.LevelWarn, "wal_disabled_no_path")
	}

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
			a.shutdown(ctx)
			cancel()
			return nil
		}
	}
}

func (a *Agent) shutdown(ctx context.Context) error {
	a.node.Close()
	if a.wal != nil {
		a.wal.Close()
	}
	if a.conn != nil {
		a.conn.Close()
	}
	a.gateway = nil
	a.conn = nil
	// TODO: Close any other resources that need to be closed.
	// specifically, the bidirectional telemetry stream.
	return nil
}

// runMAVLink owns the lifecycle of the gomavlib node.
func (a *Agent) runMAVLink(ctx context.Context) error {
	if err := a.node.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize node: %v", err)
	}
	defer a.node.Close()

	for evt := range a.node.Events() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if frameEvt, ok := evt.(*gomavlib.EventFrame); ok {
				slog.LogAttrs(
					ctx, slog.LevelInfo,
					"mavlink_frame_received",
					slog.String("frame-message", fmt.Sprintf("%+v", frameEvt.Message())),
				)
				// TODO Handle sending to WAL here.
				// Convert to WAL should give back a TelemetryFrame.
				telemetryFrame, err := a.sendToWAL(ctx, frameEvt)
				if err != nil {
					slog.LogAttrs(
						ctx, slog.LevelError,
						"failed_to_send_to_wal",
						slog.String("error", err.Error()),
					)
					// TODO: when we error here, we probably want to retry with a
					// error queue.
					// We should also consider backoff.
					continue
				}

				a.eventFrameQueue <- telemetryFrame
				continue
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

			slog.LogAttrs(ctx, slog.LevelError, "mavlink_unsupported_event", slog.String("event-type", fmt.Sprintf("%T", evt)))
		}
	}

	return nil
}

func (a *Agent) sendToWAL(ctx context.Context, frame *gomavlib.EventFrame) (*QueuedFrame, error) {
	// Serialize the frame message to JSON (or binary if preferred/available)
	payload, err := json.Marshal(frame.Message())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal frame message: %w", err)
	}

	msgName := fmt.Sprintf("%T", frame.Message())

	// Construct the TelemetryFrame to return
	tFrame := &agentv1.TelemetryFrame{
		RawMavlink:   payload,
		SentAtUnixNs: time.Now().UnixNano(),
		MsgName:      msgName,
		DroneId:      identity.Resolve().FinalID,
	}

	var walID int64

	// Write to WAL if configured
	if a.wal != nil {
		// NOTE: In a full production system, we might want to batch these writes
		// or handle them asynchronously to avoid blocking the MAVLink ingest loop.
		// For now, we write synchronously to ensure durability.
		id, err := a.wal.Append(ctx, payload)
		if err != nil {
			return nil, fmt.Errorf("wal append failed: %w", err)
		}
		walID = id
	}

	return &QueuedFrame{
		Frame: tFrame,
		WALID: walID,
	}, nil
}

// dialRelay establishes a gRPC connection to the relay using the configured target.
func (a *Agent) establishRelayConnection(ctx context.Context) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// TODO: Use a proper TLS config with a valid certificate.
	creds := credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: a.options.SkipTLSVerification,
	})

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
			Secure: true,
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

	regCtx := metadata.AppendToOutgoingContext(ctx, "x-agent-id", agentID)
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

	stream, err := a.gateway.TelemetryStream(ctx)
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

	return nil
}

func (a *Agent) handleTelemetryFrames(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck]) error {
	// 1. Drain WAL before accepting new telemetry (Replay Loop)
	if a.wal != nil {
		slog.LogAttrs(ctx, slog.LevelInfo, "wal_replay_starting")
		for {
			entries, err := a.wal.ReadUndelivered(ctx, 100)
			if err != nil {
				slog.LogAttrs(ctx, slog.LevelError, "wal_read_error", slog.String("error", err.Error()))
				// If we can't read the WAL, we probably shouldn't proceed with new frames either?
				// For now, let's retry with backoff or just return error to force reconnect loop.
				return err
			}

			if len(entries) == 0 {
				break
			}

			for _, entry := range entries {
				// Re-construct the frame from payload
				// Note: We might be missing metadata (MsgName) if we didn't store it in the WAL separately or in the payload.
				// The payload stored is the JSON of the MAVLink message.
				// We can try to unmarshal it or just send it as RawMavlink.
				// For v0.1, we stored RawMavlink = payload.

				// Warning: We need MsgName for the proto if possible, but we only stored payload.
				// That's acceptable for v0.1 replay.

				tFrame := &agentv1.TelemetryFrame{
					RawMavlink:   entry.Payload,
					SentAtUnixNs: time.Now().UnixNano(), // New send time? Or original?
					// Original would be better but we didn't store it in a field we can easily get back without parsing.
					// entry.CreatedAt is available from WAL.
					// Let's use that if we want, but SentAtUnixNs usually implies "time sent to network".
					DroneId: identity.Resolve().FinalID,
					MsgName: "WAL_REPLAY", // Placeholder since we lost the type info in the blob
				}

				err := stream.Send(tFrame)
				if err != nil {
					return err // connection dropped → retry later
				}

				if err := a.wal.MarkDelivered(ctx, entry.ID); err != nil {
					slog.LogAttrs(ctx, slog.LevelError, "wal_mark_delivered_error", slog.Int64("id", entry.ID), slog.String("error", err.Error()))
					// Non-fatal, but means we might replay it again.
				}
			}
		}
		slog.LogAttrs(ctx, slog.LevelInfo, "wal_replay_complete")
	}

	// 2. Handle Live Telemetry
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case queuedFrame := <-a.eventFrameQueue:
			err := stream.Send(queuedFrame.Frame)
			if err != nil {
				return err
			}

			// Mark as delivered if it was WAL-backed
			if a.wal != nil && queuedFrame.WALID > 0 {
				if err := a.wal.MarkDelivered(ctx, queuedFrame.WALID); err != nil {
					slog.LogAttrs(ctx, slog.LevelError, "wal_mark_delivered_error_live", slog.Int64("id", queuedFrame.WALID), slog.String("error", err.Error()))
				}
			}
		}
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
