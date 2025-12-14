package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/makinje/aero-arc-agent/internal/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Agent struct {
	node *gomavlib.Node

	conn    *grpc.ClientConn
	gateway agentv1.AgentGatewayClient

	options *AgentOptions

	// reconnection/backoff settings – wired from AgentOptions.
	backoffInitial  time.Duration
	backoffMax      time.Duration
	eventFrameQueue chan *agentv1.TelemetryFrame

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
		eventFrameQueue: make(chan *agentv1.TelemetryFrame, options.EventQueueSize),
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
	a.conn.Close()
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

func (a *Agent) sendToWAL(ctx context.Context, frame *gomavlib.EventFrame) (*agentv1.TelemetryFrame, error) {
	// TODO: Implement this.
	return nil, nil
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

	// TODO: Populate RegisterRequest with real identity/metadata fields once AgentOptions exposes them.
	req := &agentv1.RegisterRequest{
		AgentId: identity.Resolve().FinalID,
		// TODO: Add more fields here.
	}

	slog.LogAttrs(
		ctx, slog.LevelInfo,
		"agent_registering",
		slog.String("target", a.options.RelayTarget),
	)

	_, err := a.gateway.Register(ctx, req)
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
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case frame := <-a.eventFrameQueue:
			err := stream.Send(frame)
			if err != nil {
				return err
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
