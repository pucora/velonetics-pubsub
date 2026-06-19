package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/pucora/velonetics-pubsub/v2/kafka/sasl/oauthbearer"
)

func newDialer(cluster clusterCfg) (*kafka.Dialer, error) {
	mechanism, err := buildSASLMechanism(cluster.SASL)
	if err != nil {
		return nil, err
	}

	if cluster.SASL.needsCustomConnect() && mechanism != nil {
		dialer, err := newBaseDialer(cluster)
		if err != nil {
			return nil, err
		}
		tcpDialer := tcpDialerFrom(dialer)
		saslCfg := cluster.SASL
		dialer.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			return dialPreAuthenticatedNetConn(ctx, network, address, tcpDialer, mechanism, saslCfg)
		}
		return dialer, nil
	}

	dialer, err := newBaseDialer(cluster)
	if err != nil {
		return nil, err
	}
	if mechanism != nil {
		dialer.SASLMechanism = mechanism
	}
	return dialer, nil
}

func newBaseDialer(cluster clusterCfg) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout:   parseDuration(cluster.DialTimeout, 30*time.Second),
		DualStack: true,
		ClientID:  cluster.ClientID,
		KeepAlive: parseDuration(cluster.KeepAlive, 0),
	}

	tlsCfg, err := buildTLSFromCluster(cluster)
	if err != nil {
		return nil, err
	}
	dialer.TLS = tlsCfg
	return dialer, nil
}

func tcpDialerFrom(d *kafka.Dialer) *kafka.Dialer {
	copy := *d
	copy.DialFunc = nil
	copy.SASLMechanism = nil
	return &copy
}

func buildTLSFromCluster(cluster clusterCfg) (*tls.Config, error) {
	if cluster.ClientTLS == nil {
		return nil, nil
	}
	return buildTLS(cluster.ClientTLS)
}

func buildSASLMechanism(cfg saslCfg) (sasl.Mechanism, error) {
	if !cfg.configured() {
		return nil, nil
	}

	mechanism := cfg.Mechanism
	if mechanism == "" {
		mechanism = "PLAIN"
	}

	switch mechanism {
	case "PLAIN":
		user := cfg.User
		if user == "" && cfg.AzureEventHub {
			user = "$ConnectionString"
		}
		return plainMechanism{
			Username: user,
			Password: cfg.Password,
			AuthzID:  cfg.AuthIdentity,
		}, nil
	case "OAUTHBEARER":
		return oauthbearer.Mechanism{
			TokenFunc: oauthbearer.StaticToken(cfg.Password),
			AuthzID:   cfg.AuthIdentity,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q", mechanism)
	}
}

type plainMechanism struct {
	Username string
	Password string
	AuthzID  string
}

func (plainMechanism) Name() string { return "PLAIN" }

func (m plainMechanism) Start(context.Context) (sasl.StateMachine, []byte, error) {
	if m.AuthzID != "" {
		return m, []byte(fmt.Sprintf("\x00%s\x00%s\x00%s", m.AuthzID, m.Username, m.Password)), nil
	}
	return m, []byte(fmt.Sprintf("\x00%s\x00%s", m.Username, m.Password)), nil
}

func (m plainMechanism) Next(context.Context, []byte) (bool, []byte, error) {
	return true, nil, nil
}

func buildTLS(raw map[string]interface{}) (*tls.Config, error) {
	cfg := &tls.Config{}
	if v, ok := raw["allow_insecure_connections"].(bool); ok {
		cfg.InsecureSkipVerify = v
	}
	if v, ok := raw["disable_system_ca_pool"].(bool); ok && v {
		cfg.RootCAs = x509.NewCertPool()
	} else {
		cfg.RootCAs, _ = x509.SystemCertPool()
		if cfg.RootCAs == nil {
			cfg.RootCAs = x509.NewCertPool()
		}
	}
	if ca, ok := raw["ca_certs"].([]interface{}); ok {
		for _, item := range ca {
			path, ok := item.(string)
			if !ok {
				continue
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			cfg.RootCAs.AppendCertsFromPEM(data)
		}
	}
	if certs, ok := raw["client_certs"].([]interface{}); ok {
		for _, item := range certs {
			entry, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			certPath, _ := entry["certificate"].(string)
			keyPath, _ := entry["private_key"].(string)
			if certPath == "" || keyPath == "" {
				continue
			}
			tlsCert, err := tls.LoadX509KeyPair(certPath, keyPath)
			if err != nil {
				return nil, err
			}
			cfg.Certificates = append(cfg.Certificates, tlsCert)
		}
	}
	if cert, ok := raw["client_cert"].(string); ok {
		key, _ := raw["client_key"].(string)
		if cert != "" && key != "" {
			tlsCert, err := tls.LoadX509KeyPair(cert, key)
			if err != nil {
				return nil, err
			}
			cfg.Certificates = append(cfg.Certificates, tlsCert)
		}
	}
	if sn, ok := raw["server_name"].(string); ok {
		cfg.ServerName = sn
	}
	return cfg, nil
}

func parseDuration(raw string, fallback time.Duration) time.Duration {
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return d
}

func requiredAcks(raw string) kafka.RequiredAcks {
	switch raw {
	case "no_response":
		return kafka.RequireNone
	case "wait_for_all":
		return kafka.RequireAll
	default:
		return kafka.RequireOne
	}
}

func compression(codec string) kafka.Compression {
	switch codec {
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	default:
		return 0
	}
}

func isolationLevel(raw string) kafka.IsolationLevel {
	switch raw {
	case "read_uncommited", "read_uncommitted":
		return kafka.ReadUncommitted
	default:
		return kafka.ReadCommitted
	}
}
