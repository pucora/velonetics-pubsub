package kafka

import (
	"testing"

	"github.com/pucora/lura/v2/config"
)

func TestParseAsyncDriverConfig(t *testing.T) {
	cfg, err := ParseAsyncDriverConfig(config.ExtraConfig{
		AsyncDriverNamespace: map[string]interface{}{
			"cluster": map[string]interface{}{
				"brokers": []interface{}{"localhost:9092"},
			},
			"group": map[string]interface{}{
				"group_id": "my_group",
			},
			"key_meta": "Message-Id",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Cluster.Brokers) != 1 || cfg.Cluster.Brokers[0] != "localhost:9092" {
		t.Fatalf("unexpected brokers: %+v", cfg.Cluster.Brokers)
	}
	if cfg.Group.resolvedID() != "my_group" {
		t.Fatalf("unexpected group id: %s", cfg.Group.resolvedID())
	}
	if cfg.KeyMeta != "Message-Id" {
		t.Fatalf("unexpected key_meta: %s", cfg.KeyMeta)
	}
}

func TestParseAsyncDriverConfig_notFound(t *testing.T) {
	_, err := ParseAsyncDriverConfig(config.ExtraConfig{})
	if err != ErrAsyncDriverNotFound {
		t.Fatalf("expected ErrAsyncDriverNotFound, got %v", err)
	}
}

func TestParseAsyncDriverConfig_missingBrokers(t *testing.T) {
	_, err := ParseAsyncDriverConfig(config.ExtraConfig{
		AsyncDriverNamespace: map[string]interface{}{
			"cluster": map[string]interface{}{},
		},
	})
	if err == nil {
		t.Fatal("expected error for missing brokers")
	}
}
