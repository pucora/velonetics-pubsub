package kafka

import "github.com/segmentio/kafka-go"

// ClusterConfig is the shared Kafka cluster connection settings used by
// advanced pub/sub backends and async/kafka agents.
type ClusterConfig struct {
	Brokers   []string
	ClientTLS map[string]interface{}
	SASL      SASLConfig
	ClientID  string
	DialTimeout string
	ReadTimeout string
	WriteTimeout string
	KeepAlive string
}

// SASLConfig holds SASL authentication settings.
type SASLConfig struct {
	Mechanism       string
	AzureEventHub   bool
	DisableHanshake *bool
	AuthIdentity    string
	User            string
	Password        string
}

// NewDialerFromConfig builds a kafka-go dialer from shared cluster settings.
func NewDialerFromConfig(cluster ClusterConfig) (*kafka.Dialer, error) {
	return newDialer(clusterCfg{
		Brokers:      cluster.Brokers,
		ClientTLS:    cluster.ClientTLS,
		SASL: saslCfg{
			Mechanism:       cluster.SASL.Mechanism,
			AzureEventHub:   cluster.SASL.AzureEventHub,
			DisableHanshake: cluster.SASL.DisableHanshake,
			AuthIdentity:    cluster.SASL.AuthIdentity,
			User:            cluster.SASL.User,
			Password:        cluster.SASL.Password,
		},
		DialTimeout:  cluster.DialTimeout,
		ReadTimeout:  cluster.ReadTimeout,
		WriteTimeout: cluster.WriteTimeout,
		KeepAlive:    cluster.KeepAlive,
		ClientID:     cluster.ClientID,
	})
}

// IsolationLevel maps schema isolation level values to kafka-go constants.
func IsolationLevel(raw string) kafka.IsolationLevel {
	return isolationLevel(raw)
}
