package resolver

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

type Resolver struct {
	nc *nats.Conn
}

func NewResolver(url string, cert string, key string, ca string, userJWT []byte, userKp nkeys.KeyPair) (*Resolver, error) {
	nc, err := createConnection(
		url,
		cert,
		key,
		ca,
		userJWT,
		userKp,
	)
	if err != nil {
		return nil, err
	}

	return &Resolver{
		nc: nc,
	}, nil
}
