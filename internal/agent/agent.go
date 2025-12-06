package agent

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Agent struct {
	node *gomavlib.Node

	conn    *grpc.ClientConn
	gateway agentv1.AgentGatewayClient

	options *AgentOptions

	// reconnection/backoff settings – wired from AgentOptions.
	backoffInitial time.Duration
	backoffMax     time.Duration
}

func NewAgent(options *AgentOptions) (*Agent, error) {
	if options.BackoffInitial <= 0 {
		options.BackoffInitial = time.Second
	}
	if options.BackoffMax <= 0 {
		options.BackoffMax = 30 * time.Second
	}

	return &Agent{
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
	}, nil
}

// Start runs the MAVLink ingest loop and the gRPC reconnect/stream lifecycle
// until the provided context is cancelled or a fatal error occurs.
func (a *Agent) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)

	go func() {
		errCh <- a.runMAVLink(ctx)
	}()

	go func() {
		errCh <- a.runWithReconnect(ctx)
	}()

	// Return on first error; the cancel will cause the other loop to exit.
	err := <-errCh
	cancel()

	return err
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
				// TODO: Handle frame (enqueue for telemetry pipeline).
				slog.LogAttrs(
					ctx, slog.LevelInfo,
					"mavlink_frame_received",
					slog.String("frame-message", fmt.Sprintf("%+v", frameEvt.Message())),
				)
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

// dialRelay establishes a gRPC connection to the relay using the configured target.
func (a *Agent) dialRelay(ctx context.Context) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	slog.LogAttrs(
		dialCtx, slog.LevelInfo,
		"agent_connecting",
		slog.String("target", a.options.RelayTarget),
	)

	conn, err := grpc.DialContext(
		dialCtx,
		a.options.RelayTarget,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
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
	req := &agentv1.RegisterRequest{}

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
func (a *Agent) runStreamLoop(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck]) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			ack, err := stream.Recv()
			if err != nil {
				return err
			}

			slog.LogAttrs(
				ctx, slog.LevelDebug,
				"telemetry_ack_received",
				slog.String("ack", fmt.Sprintf("%+v", ack)),
			)
		}
	}
}

// runWithReconnect orchestrates connect → register → stream with exponential
// backoff and context-aware cancellation.
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

		conn, err := a.dialRelay(ctx)
		if err != nil {
			slog.LogAttrs(
				ctx, slog.LevelError,
				"agent_connect_failed",
				slog.String("target", a.options.RelayTarget),
				slog.String("error", err.Error()),
				slog.Int64("backoff_ms", backoff.Milliseconds()),
			)

			if !sleepWithContext(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		a.conn = conn
		a.gateway = agentv1.NewAgentGatewayClient(conn)

		regCtx, cancelReg := context.WithTimeout(ctx, 10*time.Second)
		err = a.register(regCtx)
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

			if !sleepWithContext(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		stream, err := a.openTelemetryStream(ctx)
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

			if !sleepWithContext(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		err = a.runStreamLoop(ctx, stream)

		slog.LogAttrs(
			ctx, slog.LevelInfo,
			"agent_stream_closed",
			slog.String("target", a.options.RelayTarget),
			slog.String("error", fmt.Sprintf("%v", err)),
		)

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

			if !sleepWithContext(ctx, backoff) {
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
