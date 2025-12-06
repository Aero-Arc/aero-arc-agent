package agent

import (
	"fmt"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"google.golang.org/grpc"
)

type Agent struct {
	Node   *gomavlib.Node
	Client agentv1.AgentGatewayClient
}

func NewAgent(options *AgentOptions) (*Agent, error) {
	conn, err := grpc.NewClient("localhost:50051")
	if err != nil {
		return nil, ErrFailedToConnectToServer
	}

	return &Agent{
		Client: agentv1.NewAgentGatewayClient(conn),
		Node: &gomavlib.Node{
			Endpoints: []gomavlib.EndpointConf{
				gomavlib.EndpointSerial{
					Device: options.SerialPath,
					Baud:   options.SerialBaud,
				},
			},
			Dialect: common.Dialect,
		},
	}, nil
}

func (a *Agent) Start() error {
	return fmt.Errorf("not implemented")
}
