package agent

import (
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v3"
)

type AgentOptions struct {
	SerialPath    string
	SerialBaud    int
	ServerAddress string
	ServerPort    int

	// Computed relay target in host:port form.
	RelayTarget string

	// Reconnect/backoff configuration.
	BackoffInitial time.Duration
	BackoffMax     time.Duration

	ConsulAddress string
	ConsulPort    int
	ConsulToken   string
	ConsulAgentID string

	APIKey              string
	EventQueueSize      int
	SkipTLSVerification bool
	WALPath             string
	WALBatchSize        int64
	WALFlushTimeout     time.Duration
	Debug               bool
}

func GetAgentOptions(c *cli.Command) (*AgentOptions, error) {
	if c.IsSet("consul-address") || c.IsSet("consul-port") || c.IsSet("consul-token") || c.IsSet("consul-agent-id") {
		return nil, ErrConsulUnsupported
	}

	return &AgentOptions{
		SerialPath:    c.String("serial-path"),
		SerialBaud:    c.Int("serial-baud"),
		ServerAddress: c.String("server-address"),
		ServerPort:    c.Int("server-port"),
		RelayTarget:   fmt.Sprintf("%s:%d", c.String("server-address"), c.Int("server-port")),

		BackoffInitial:      c.Duration("backoff-initial"),
		BackoffMax:          c.Duration("backoff-max"),
		APIKey:              os.Getenv("AERO_ARC_API_KEY"),
		EventQueueSize:      c.Int("event-queue-size"),
		WALPath:             c.String("wal-path"),
		WALBatchSize:        c.Int64("wal-batch-size"),
		WALFlushTimeout:     c.Duration("wal-flush-timeout"),
		SkipTLSVerification: c.Bool("skip-tls-verification"),
		Debug:               c.Bool("debug"),
	}, nil
}
