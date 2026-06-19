package kafka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/pucora/lura/v2/config"
)

// AsyncDriverNamespace is the extra_config key for async/kafka agents.
const AsyncDriverNamespace = "github.com/pucora/velonetics-pubsub/async"

// AsyncReaderConfig defines the async/kafka driver settings.
type AsyncReaderConfig struct {
	Cluster clusterCfg `json:"cluster"`
	Group   groupCfg   `json:"group"`
	KeyMeta string     `json:"key_meta"`
}

// ParseAsyncDriverConfig reads async/kafka settings from an async agent extra_config.
func ParseAsyncDriverConfig(extra config.ExtraConfig) (*AsyncReaderConfig, error) {
	cfg, ok := extra[AsyncDriverNamespace]
	if !ok {
		return nil, ErrAsyncDriverNotFound
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	var out AsyncReaderConfig
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	if len(out.Cluster.Brokers) == 0 {
		return nil, fmt.Errorf("async/kafka cluster.brokers is required")
	}
	return &out, nil
}

// ErrAsyncDriverNotFound is returned when async/kafka is not configured.
var ErrAsyncDriverNotFound = fmt.Errorf("async/kafka config not found")

// NewAsyncReader builds a Kafka reader for background async agents.
// It follows the latest-offset policy: only messages produced after startup are consumed.
func NewAsyncReader(cfg AsyncReaderConfig, topic, defaultGroupID string) (*kafka.Reader, error) {
	dialer, err := newDialer(cfg.Cluster)
	if err != nil {
		return nil, err
	}

	groupID := cfg.Group.resolvedID()
	if groupID == "" {
		groupID = defaultGroupID
	}

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:           cfg.Cluster.Brokers,
		GroupID:           groupID,
		Topic:             topic,
		Dialer:            dialer,
		StartOffset:       kafka.LastOffset,
		IsolationLevel:    isolationLevel(cfg.Group.IsolationLevel),
		SessionTimeout:    parseDuration(cfg.Group.SessionTimeout, 10*time.Second),
		HeartbeatInterval: parseDuration(cfg.Group.HeartbeatInterval, 3*time.Second),
	}), nil
}

// ToClusterConfig converts async cluster settings to the shared cluster config type.
func (c clusterCfg) ToClusterConfig() ClusterConfig {
	return ClusterConfig{
		Brokers:      c.Brokers,
		ClientTLS:    c.ClientTLS,
		SASL: SASLConfig{
			Mechanism:       c.SASL.Mechanism,
			AzureEventHub:   c.SASL.AzureEventHub,
			DisableHanshake: c.SASL.DisableHanshake,
			AuthIdentity:    c.SASL.AuthIdentity,
			User:            c.SASL.User,
			Password:        c.SASL.Password,
		},
		ClientID:     c.ClientID,
		DialTimeout:  c.DialTimeout,
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
		KeepAlive:    c.KeepAlive,
	}
}
