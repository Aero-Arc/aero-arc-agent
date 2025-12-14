package agent

import "context"

type TokenAuth struct {
	Token  string
	Secure bool // set to true to require TLS
}

func (t TokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + t.Token,
	}, nil
}

func (t TokenAuth) RequireTransportSecurity() bool {
	return t.Secure
}
