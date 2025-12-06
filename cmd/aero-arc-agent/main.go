package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/makinje/aero-arc-agent/internal/agent"
	"github.com/urfave/cli/v3"
	"google.golang.org/grpc"
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
	},
}

func RunAgent(ctx context.Context, cmd *cli.Command) error {
	options, err := agent.GetAgentOptions(cmd)
	if err != nil {
		return fmt.Errorf("failed to get agent options: %v", err)
	}

	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", options.ServerAddress, options.ServerPort))
	if err != nil {
		return fmt.Errorf("failed to connect to server: %v", err)
	}

	agent.NewAgent(options)

	defer conn.Close()

	return nil
}

func main() {
	if err := agentCmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
