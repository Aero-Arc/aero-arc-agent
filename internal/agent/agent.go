package agent

import (
	"context"
	"fmt"
	"log/slog"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"google.golang.org/grpc"
)

type Agent struct {
	node    *gomavlib.Node
	gateway agentv1.AgentGatewayClient
	options *AgentOptions
}

func NewAgent(options *AgentOptions) (*Agent, error) {
	conn, err := grpc.NewClient("localhost:50051")
	if err != nil {
		return nil, ErrFailedToConnectToServer
	}

	return &Agent{
		gateway: agentv1.NewAgentGatewayClient(conn),
		node: &gomavlib.Node{
			Endpoints: []gomavlib.EndpointConf{
				gomavlib.EndpointSerial{
					Device: options.SerialPath,
					Baud:   options.SerialBaud,
				},
			},
			Dialect: common.Dialect,
		},
		options: options,
	}, nil
}

func (a *Agent) Start(ctx context.Context) error {
	if err := a.node.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize node: %v", err)
	}

	for evt := range a.node.Events() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if frameEvt, ok := evt.(*gomavlib.EventFrame); ok {
				// TODO: Handle frame
				slog.LogAttrs(
					ctx, slog.LevelInfo,
					"Received frame",
					slog.String("frame-message", fmt.Sprintf("%+v", frameEvt.Message())),
				)
				continue
			}

			if _, ok := evt.(*gomavlib.EventChannelOpen); ok {
				// TODO: Handle channel open
				slog.LogAttrs(
					ctx, slog.LevelInfo,
					"Channel opened: for relay",
					slog.String("relay-address", a.options.ServerAddress),
					slog.Int("relay-port", a.options.ServerPort),
				)
				continue
			}

			if _, ok := evt.(*gomavlib.EventChannelClose); ok {
				// TODO: Handle channel close
				slog.LogAttrs(
					ctx, slog.LevelInfo,
					"Channel closed: for relay",
					slog.String("relay-address", a.options.ServerAddress),
					slog.Int("relay-port", a.options.ServerPort),
				)
				continue
			}

			slog.LogAttrs(ctx, slog.LevelError, "unsupported event type", slog.String("event-type", fmt.Sprintf("%T", evt)))
		}
	}

	return nil
}
