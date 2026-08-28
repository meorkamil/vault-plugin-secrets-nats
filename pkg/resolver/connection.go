package resolver

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/rs/zerolog/log"
)

func (r *Resolver) CloseConnection() {
	if r != nil {
		if r.nc != nil {
			r.nc.Close()
		}
	}
}

func isNatsUrl(url string) bool {
	url = strings.ToLower(strings.TrimSpace(url))
	return strings.HasPrefix(url, "nats://") || strings.HasPrefix(url, ",nats://") || strings.HasPrefix(url, "tls://") || strings.HasPrefix(url, ",tls://")

}

func createConnection(url string, cert string, key string, ca string, userJWT []byte, userKp nkeys.KeyPair) (*nats.Conn, error) {
	if !isValidURL(url) {
		return nil, fmt.Errorf("invalid url: %s", url)
	}

	if !isNatsUrl(url) {
		return nil, fmt.Errorf("invalid url: %s, currently only nats urls are supported", url)
	}

	nats.NewInbox()
	getOpt := func(theJWT string, kp nkeys.KeyPair) nats.Option {
		return nats.UserJWT(
			func() (string, error) {
				return theJWT, nil
			}, func(nonce []byte) ([]byte, error) {
				return kp.Sign(nonce)
			})
	}

	// Set default TLS as false. Only enable when url contain "tls"
	tlsEnabled := false
	if strings.Contains(url, "tls") {
		tlsEnabled = true
	}

	return nats.Connect(url, createDefaultToolOptions("nsc_push", cert, key, ca, tlsEnabled, getOpt(string(userJWT), userKp))...)
}

func createDefaultToolOptions(name string, cert string, key string, ca string, tlsEnabled bool, o ...nats.Option) []nats.Option {
	connectTimeout := 5 * time.Second
	totalWait := 10 * time.Minute
	reconnectDelay := 2 * time.Second

	opts := []nats.Option{nats.Name(name)}
	opts = append(opts, nats.Timeout(connectTimeout))

	// Set TLS Configuration
	if tlsEnabled {
		log.Info().Msgf("TLS Configuration: cert: %s, key: %s, ca: %s", cert, key, ca)
		opts = append(opts, nats.RootCAs(ca))
		opts = append(opts, nats.ClientCert(cert, key))
	}

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		if err != nil {
			log.Error().Msgf("Disconnected: error: %v\n", err)
		}
		if nc.Status() == nats.CLOSED {
			return
		}
		log.Info().Msgf("Disconnected: will attempt reconnects for %.0fm", totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Info().Msgf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		if nc.Status() == nats.CLOSED {
			return
		}
		log.Info().Msgf("Exiting, no servers available, or connection closed")
	}))
	opts = append(opts, o...)
	return opts
}

func isValidURL(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}

	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	supported := []string{"http", "https", "nats", "tls"}

	ok := false
	for _, v := range supported {
		if scheme == v {
			ok = true
			break
		}
	}
	return ok
}
