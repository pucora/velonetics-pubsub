package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
	"github.com/segmentio/kafka-go/protocol/saslauthenticate"
	"github.com/segmentio/kafka-go/protocol/saslhandshake"
	"github.com/segmentio/kafka-go/sasl"
)

type connectOptions struct {
	azureEventHub bool
	sendHandshake bool
}

type protocolBrokerConn struct {
	net.Conn
	pc *protocol.Conn
}

func (c *protocolBrokerConn) Close() error {
	return c.Conn.Close()
}

func (s saslCfg) needsCustomConnect() bool {
	return s.AzureEventHub || !s.sendHandshake()
}

func (s saslCfg) sendHandshake() bool {
	// KrakenD schema default is true: send the Kafka SASL handshake.
	if s.DisableHanshake == nil {
		return true
	}
	return *s.DisableHanshake
}

func (s saslCfg) configured() bool {
	mechanism := s.Mechanism
	if mechanism == "" {
		mechanism = "PLAIN"
	}
	switch mechanism {
	case "OAUTHBEARER":
		return s.Password != ""
	default:
		return s.User != "" || s.Password != ""
	}
}

func openBrokerProtocolConn(
	ctx context.Context,
	network, address string,
	dialer *kafka.Dialer,
	mechanism sasl.Mechanism,
	opts connectOptions,
) (*protocolBrokerConn, error) {
	if dialer.Timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dialer.Timeout)
		defer cancel()
	}

	netConn, err := dialTCP(ctx, network, address, dialer)
	if err != nil {
		return nil, err
	}

	clientID := dialer.ClientID
	if clientID == "" {
		clientID = "velonetics-pubsub"
	}

	pc := protocol.NewConn(netConn, clientID)
	deadline, _ := ctx.Deadline()
	if !deadline.IsZero() {
		pc.SetDeadline(deadline)
	}

	versions, err := negotiateAPIVersions(pc)
	if err != nil {
		_ = netConn.Close()
		return nil, err
	}

	if opts.azureEventHub {
		versions = capAPIVersionsForAzure(versions)
	}
	pc.SetVersions(versions)
	pc.SetDeadline(time.Time{})

	if mechanism != nil {
		if err := authenticateBroker(ctx, pc, mechanism, opts.sendHandshake, versions); err != nil {
			_ = netConn.Close()
			return nil, err
		}
	}

	return &protocolBrokerConn{Conn: netConn, pc: pc}, nil
}

func dialPreAuthenticatedNetConn(
	ctx context.Context,
	network, address string,
	dialer *kafka.Dialer,
	mechanism sasl.Mechanism,
	saslCfg saslCfg,
) (net.Conn, error) {
	bc, err := openBrokerProtocolConn(ctx, network, address, dialer, mechanism, connectOptions{
		azureEventHub: saslCfg.AzureEventHub,
		sendHandshake: saslCfg.sendHandshake(),
	})
	if err != nil {
		return nil, err
	}
	return bc.Conn, nil
}

func capAPIVersionsForAzure(versions map[protocol.ApiKey]int16) map[protocol.ApiKey]int16 {
	out := make(map[protocol.ApiKey]int16, len(versions))
	for k, v := range versions {
		out[k] = v
	}
	out[protocol.SaslHandshake] = 0
	delete(out, protocol.SaslAuthenticate)
	return out
}

func dialTCP(ctx context.Context, network, address string, dialer *kafka.Dialer) (net.Conn, error) {
	dial := dialer.DialFunc
	if dial == nil {
		nd := &net.Dialer{
			LocalAddr: dialer.LocalAddr,
			Timeout:   dialer.Timeout,
			KeepAlive: dialer.KeepAlive,
		}
		if dialer.DualStack {
			nd.FallbackDelay = dialer.FallbackDelay
		}
		dial = nd.DialContext
	}

	resolved, err := resolveAddress(ctx, address, dialer.Resolver)
	if err != nil {
		return nil, err
	}

	conn, err := dial(ctx, network, resolved)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to %s: %w", resolved, err)
	}

	if dialer.TLS != nil {
		tlsCfg := dialer.TLS
		if tlsCfg.ServerName == "" {
			tlsCfg = tlsCfg.Clone()
			host, _, _ := net.SplitHostPort(resolved)
			if host == "" {
				host = resolved
			}
			tlsCfg.ServerName = host
		}
		conn = tls.Client(conn, tlsCfg)
	}

	return conn, nil
}

func resolveAddress(ctx context.Context, address string, resolver kafka.Resolver) (string, error) {
	if resolver == nil {
		return address, nil
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return address, nil
	}
	addresses, err := resolver.LookupHost(ctx, host)
	if err != nil {
		return "", err
	}
	if len(addresses) == 0 {
		return address, nil
	}
	if port == "" {
		port = "9092"
	}
	return net.JoinHostPort(addresses[0], port), nil
}

func negotiateAPIVersions(pc *protocol.Conn) (map[protocol.ApiKey]int16, error) {
	msg, err := pc.RoundTrip(&apiversions.Request{})
	if err != nil {
		return nil, err
	}
	res := msg.(*apiversions.Response)
	if res.ErrorCode != 0 {
		return nil, kafka.Error(res.ErrorCode)
	}

	versions := make(map[protocol.ApiKey]int16, len(res.ApiKeys))
	for _, key := range res.ApiKeys {
		apiKey := protocol.ApiKey(key.ApiKey)
		versions[apiKey] = apiKey.SelectVersion(key.MinVersion, key.MaxVersion)
	}
	return versions, nil
}

func authenticateBroker(
	ctx context.Context,
	pc *protocol.Conn,
	mechanism sasl.Mechanism,
	sendHandshake bool,
	versions map[protocol.ApiKey]int16,
) error {
	if sendHandshake {
		if err := saslHandshake(pc, mechanism.Name()); err != nil {
			return err
		}
	}

	sess, state, err := mechanism.Start(ctx)
	if err != nil {
		return err
	}

	for completed := false; !completed; {
		challenge, err := saslAuthenticate(pc, state, versions)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return kafka.SASLAuthenticationFailed
			}
			return err
		}

		completed, state, err = sess.Next(ctx, challenge)
		if err != nil {
			return err
		}
	}
	return nil
}

func saslHandshake(pc *protocol.Conn, mechanism string) error {
	msg, err := pc.RoundTrip(&saslhandshake.Request{Mechanism: mechanism})
	if err != nil {
		return err
	}
	res := msg.(*saslhandshake.Response)
	if res.ErrorCode != 0 {
		return kafka.Error(res.ErrorCode)
	}
	return nil
}

func saslAuthenticate(pc *protocol.Conn, data []byte, versions map[protocol.ApiKey]int16) ([]byte, error) {
	req := &saslauthenticate.Request{AuthBytes: data}
	if req.Required(versions) {
		msg, err := req.RawExchange(pc)
		if err != nil {
			return nil, err
		}
		res := msg.(*saslauthenticate.Response)
		return res.AuthBytes, nil
	}

	msg, err := pc.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	res := msg.(*saslauthenticate.Response)
	if res.ErrorCode != 0 {
		return nil, kafka.Error(res.ErrorCode)
	}
	return res.AuthBytes, nil
}

func newWriterTransport(cluster clusterCfg) (kafka.RoundTripper, error) {
	mechanism, err := buildSASLMechanism(cluster.SASL)
	if err != nil {
		return nil, err
	}

	if cluster.SASL.needsCustomConnect() {
		return &customRoundTripper{cluster: cluster, mechanism: mechanism}, nil
	}

	tlsCfg, err := buildTLSFromCluster(cluster)
	if err != nil {
		return nil, err
	}

	return &kafka.Transport{
		TLS:         tlsCfg,
		SASL:        mechanism,
		ClientID:    cluster.ClientID,
		DialTimeout: parseDuration(cluster.DialTimeout, 30*time.Second),
	}, nil
}

type customRoundTripper struct {
	cluster   clusterCfg
	mechanism sasl.Mechanism
}

func (t *customRoundTripper) RoundTrip(ctx context.Context, addr net.Addr, req kafka.Request) (kafka.Response, error) {
	dialer, err := newBaseDialer(t.cluster)
	if err != nil {
		return nil, err
	}

	bc, err := openBrokerProtocolConn(ctx, addr.Network(), addr.String(), dialer, t.mechanism, connectOptions{
		azureEventHub: t.cluster.SASL.AzureEventHub,
		sendHandshake: t.cluster.SASL.sendHandshake(),
	})
	if err != nil {
		return nil, err
	}
	defer bc.Close()

	if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
		bc.pc.SetDeadline(deadline)
		defer bc.pc.SetDeadline(time.Time{})
	}

	return bc.pc.RoundTrip(req)
}
