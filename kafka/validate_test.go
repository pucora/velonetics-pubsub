package kafka

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/velonetics/lura/v2/config"
)

func TestIsolationLevel(t *testing.T) {
	cases := []struct {
		raw  string
		want kafka.IsolationLevel
	}{
		{"read_uncommited", kafka.ReadUncommitted},
		{"read_uncommitted", kafka.ReadUncommitted},
		{"read_commited", kafka.ReadCommitted},
		{"read_committed", kafka.ReadCommitted},
		{"", kafka.ReadCommitted},
	}
	for _, tc := range cases {
		if got := isolationLevel(tc.raw); got != tc.want {
			t.Fatalf("isolationLevel(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}

func TestValidateConfig_kafkaPublisherMissingBrokers(t *testing.T) {
	cfg := &config.ServiceConfig{
		Endpoints: []*config.EndpointConfig{{
			Endpoint: "/publish",
			Backend: []*config.Backend{{
				URLPattern: "/ignored",
				ExtraConfig: config.ExtraConfig{
					PublisherNamespace: map[string]interface{}{
						"writer": map[string]interface{}{
							"topic": "events",
							"cluster": map[string]interface{}{},
						},
					},
				},
			}},
		}},
	}
	if err := ValidateConfig(cfg); err == nil {
		t.Fatal("expected validation error for missing brokers")
	}
}

func TestValidateConfig_asyncKafka(t *testing.T) {
	cfg := &config.ServiceConfig{
		AsyncAgents: []*config.AsyncAgent{{
			Name: "events",
			ExtraConfig: config.ExtraConfig{
				AsyncDriverNamespace: map[string]interface{}{
					"cluster": map[string]interface{}{
						"brokers": []interface{}{"localhost:9092"},
					},
				},
			},
		}},
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
}

func TestValidateConfig_kafkaSubscriberMultipleTopics(t *testing.T) {
	cfg := &config.ServiceConfig{
		Endpoints: []*config.EndpointConfig{{
			Endpoint: "/subscribe",
			Backend: []*config.Backend{{
				URLPattern: "/ignored",
				ExtraConfig: config.ExtraConfig{
					SubscriberNamespace: map[string]interface{}{
						"reader": map[string]interface{}{
							"topics": []interface{}{"a", "b"},
							"cluster": map[string]interface{}{
								"brokers": []interface{}{"localhost:9092"},
							},
						},
					},
				},
			}},
		}},
	}
	if err := ValidateConfig(cfg); err == nil {
		t.Fatal("expected validation error for multiple topics")
	}
}
