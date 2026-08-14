package agent

import "context"

type TokenAuth struct {
	Token  string
	Secure bool // set to true to require TLS
}

// GetRequestMetadata returns the authentication metadata attached to each outgoing Agent RPC.
//
// Parameters:
//   - ctx: controls cancellation and deadlines for the operation.
//   - uri: is accepted by gRPC's credential interface and is not used when
//     constructing the bearer credential.
//
// Returns:
//   - metadata: contains one authorization header using the configured token.
//   - error: is always nil; the signature satisfies credentials.PerRPCCredentials.
func (t TokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + t.Token,
	}, nil
}

// RequireTransportSecurity reports whether Agent RPC credentials require a transport-secured connection.
//
// Returns:
//   - secure: mirrors TokenAuth.Secure and tells gRPC whether TLS is mandatory.
func (t TokenAuth) RequireTransportSecurity() bool {
	return t.Secure
}
