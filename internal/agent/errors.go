package agent

import "errors"

var (
	ErrSerialPathNotSet          = errors.New("serial path not set")
	ErrSerialBaudNotSet          = errors.New("serial baud not set")
	ErrServerAddressNotSet       = errors.New("server address not set")
	ErrServerPortNotSet          = errors.New("server port not set")
	ErrConsulAddressNotSet       = errors.New("consul address not set")
	ErrConsulPortNotSet          = errors.New("consul port not set")
	ErrConsulTokenNotSet         = errors.New("consul token not set")
	ErrConsulAgentIDNotSet       = errors.New("consul agent ID not set")
	ErrConsulUnsupported         = errors.New("consul unsupported")
	ErrFailedToConnectToServer   = errors.New("failed to connect to server")
	ErrFailedRelayGrpcConnection = errors.New("failed to establish relay grpc connection")

	ErrGatewayNotInitialized = errors.New("agent gateway client not initialized")
)
