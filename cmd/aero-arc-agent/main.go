package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/makinje/aero-arc-agent/internal/agent"
	"github.com/urfave/cli/v3"
)

var agentCmd = cli.Command{
	Name:   "run",
	Usage:  "run the agent edge process",
	Action: RunAgent,

	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "serial-path",
			Value: "/dev/ttyUSB0",
			Usage: "The serial path to use for the agent",
		},
		&cli.IntFlag{
			Name:  "serial-baud",
			Value: 115200,
			Usage: "The baud rate to use for the serial connection",
		},
		&cli.StringFlag{
			Name:  "server-address",
			Value: "localhost",
			Usage: "The address of the server to connect to",
		},
		&cli.IntFlag{
			Name:  "server-port",
			Value: 8080,
			Usage: "The port of the server to connect to",
		},
		&cli.DurationFlag{
			Name:  "backoff-initial",
			Value: 1 * time.Second,
			Usage: "Initial reconnect backoff duration",
		},
		&cli.DurationFlag{
			Name:  "backoff-max",
			Value: 30 * time.Second,
			Usage: "Maximum reconnect backoff duration",
		},
		&cli.IntFlag{
			Name:  "event-queue-size",
			Value: 1000,
			Usage: "The size of the event queue",
		},
	},
}

func RunAgent(ctx context.Context, cmd *cli.Command) error {
	options, err := agent.GetAgentOptions(cmd)
	if err != nil {
		return fmt.Errorf("failed to get agent options: %v", err)
	}

	a, err := agent.NewAgent(options)
	if err != nil {
		return fmt.Errorf("failed to construct agent: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	return a.Start(ctx, sigCh)
}

func main() {
	if err := agentCmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
