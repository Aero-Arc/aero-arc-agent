package agent

import "github.com/urfave/cli/v3"

type AgentOptions struct {
	SerialPath    string
	SerialBaud    int
	ServerAddress string
	ServerPort    int
	ConsulAddress string
	ConsulPort    int
	ConsulToken   string
	ConsulAgentID string
}

func GetAgentOptions(c *cli.Command) (*AgentOptions, error) {
	if !c.IsSet("serial-path") {
		return nil, ErrSerialPathNotSet
	}
	if !c.IsSet("serial-baud") {
		return nil, ErrSerialBaudNotSet
	}
	if !c.IsSet("server-address") {
		return nil, ErrServerAddressNotSet
	}
	if !c.IsSet("server-port") {
		return nil, ErrServerPortNotSet
	}
	if c.IsSet("consul-address") || c.IsSet("consul-port") || c.IsSet("consul-token") || c.IsSet("consul-agent-id") {
		return nil, ErrConsulUnsupported
	}

	return &AgentOptions{
		SerialPath:    c.String("serial-path"),
		SerialBaud:    c.Int("serial-baud"),
		ServerAddress: c.String("server-address"),
		ServerPort:    c.Int("server-port"),
	}, nil
}
