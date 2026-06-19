package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/pucora/lura/v2/config"
)

const (
	PublisherNamespace  = "github.com/pucora/pucora-pubsub/kafka/publisher"
	SubscriberNamespace = "github.com/pucora/pucora-pubsub/kafka/subscriber"
)

type publisherCfg struct {
	SuccessStatusCode int          `json:"success_status_code"`
	Writer            writerCfg    `json:"writer"`
}

type subscriberCfg struct {
	Reader readerCfg `json:"reader"`
}

type writerCfg struct {
	Cluster  clusterCfg  `json:"cluster"`
	Producer producerCfg `json:"producer"`
	Topic    string      `json:"topic"`
	KeyMeta  string      `json:"key_meta"`
}

type readerCfg struct {
	Cluster clusterCfg `json:"cluster"`
	Group   groupCfg   `json:"group"`
	Topics  []string   `json:"topics"`
	KeyMeta string     `json:"key_meta"`
}

type clusterCfg struct {
	Brokers              []string               `json:"brokers"`
	ClientTLS            map[string]interface{} `json:"client_tls"`
	SASL                 saslCfg                `json:"sasl"`
	DialTimeout          string                 `json:"dial_timeout"`
	ReadTimeout          string                 `json:"read_timeout"`
	WriteTimeout         string                 `json:"write_timeout"`
	KeepAlive            string                 `json:"keep_alive"`
	ClientID             string                 `json:"client_id"`
	RackID               string                 `json:"rack_id"`
	ChannelBufferSize    int                    `json:"channel_buffer_size"`
	MetadataRetryBackoff string                 `json:"metadata_retry_backoff"`
	MetadataRetryMax     int                    `json:"metadata_retry_max"`
}

type saslCfg struct {
	Mechanism       string `json:"mechanism"`
	AzureEventHub   bool   `json:"azure_event_hub"`
	DisableHanshake *bool  `json:"disable_hanshake"`
	AuthIdentity    string `json:"auth_identity"`
	User            string `json:"user"`
	Password        string `json:"password"`
	ScramAuthID     string `json:"scram_auth_id"`
}

type producerCfg struct {
	MaxMessageBytes     int    `json:"max_message_bytes"`
	RequiredAcks        string `json:"required_acks"`
	RequiredAcksTimeout string `json:"required_acks_timeout"`
	CompressionCodec    string `json:"compression_codec"`
	CompressionLevel    string `json:"compression_level"`
	Partitioner         string `json:"partitioner"`
	Idempotent          bool   `json:"idempotent"`
	RetryMax            int    `json:"retry_max"`
	RetryBackoff        string `json:"retry_backoff"`
}

type groupCfg struct {
	ID                  string   `json:"id"`
	GroupID             string   `json:"group_id"`
	SessionTimeout      string   `json:"session_timeout"`
	HeartbeatInterval   string   `json:"heartbeat_interval"`
	RebalanceStrategies []string `json:"rebalance_strategies"`
	RebalanceTimeout    string   `json:"rebalance_timeout"`
	InstanceID          string   `json:"instance_id"`
	FetchDefault        int      `json:"fetch_default"`
	IsolationLevel      string   `json:"isolation_level"`
}

func (g groupCfg) resolvedID() string {
	if g.GroupID != "" {
		return g.GroupID
	}
	return g.ID
}

func parsePublisherConfig(remote *config.Backend) (*publisherCfg, error) {
	return parseConfig[publisherCfg](remote, PublisherNamespace)
}

func parseSubscriberConfig(remote *config.Backend) (*subscriberCfg, error) {
	return parseConfig[subscriberCfg](remote, SubscriberNamespace)
}

func parseConfig[T any](remote *config.Backend, namespace string) (*T, error) {
	cfg, ok := remote.ExtraConfig[namespace]
	if !ok {
		return nil, &namespaceNotFoundErr{Namespace: namespace}
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

type namespaceNotFoundErr struct {
	Namespace string
}

func (n *namespaceNotFoundErr) Error() string {
	return n.Namespace + " not found in the extra config"
}

func IsNamespaceNotFound(err error) bool {
	_, ok := err.(*namespaceNotFoundErr)
	return ok
}

func validatePublisher(cfg *publisherCfg) error {
	if cfg.Writer.Topic == "" {
		return fmt.Errorf("writer.topic is required")
	}
	if len(cfg.Writer.Cluster.Brokers) == 0 {
		return fmt.Errorf("writer.cluster.brokers is required")
	}
	return nil
}

func validateSubscriber(cfg *subscriberCfg) error {
	if len(cfg.Reader.Topics) == 0 {
		return fmt.Errorf("reader.topics is required")
	}
	if len(cfg.Reader.Topics) != 1 {
		return fmt.Errorf("reader.topics must contain exactly one topic for HTTP subscribers")
	}
	if len(cfg.Reader.Cluster.Brokers) == 0 {
		return fmt.Errorf("reader.cluster.brokers is required")
	}
	return nil
}
