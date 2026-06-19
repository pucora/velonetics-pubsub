package kafka

import (
	"fmt"

	"github.com/pucora/lura/v2/config"
)

// ValidateConfig checks Kafka advanced pub/sub and async/kafka settings at startup.
func ValidateConfig(cfg *config.ServiceConfig) error {
	if cfg == nil {
		return nil
	}
	for _, ep := range cfg.Endpoints {
		for _, b := range ep.Backend {
			if err := validateBackend(b); err != nil {
				return fmt.Errorf("endpoint %q backend %q: %w", ep.Endpoint, b.URLPattern, err)
			}
		}
	}
	for _, agent := range cfg.AsyncAgents {
		if _, ok := agent.ExtraConfig[AsyncDriverNamespace]; !ok {
			continue
		}
		if _, err := ParseAsyncDriverConfig(agent.ExtraConfig); err != nil {
			return fmt.Errorf("async_agent %q: %w", agent.Name, err)
		}
	}
	return nil
}

func validateBackend(b *config.Backend) error {
	if b == nil {
		return nil
	}
	if hasNamespace(b, PublisherNamespace) {
		cfg, err := parsePublisherConfig(b)
		if err != nil {
			return err
		}
		return validatePublisher(cfg)
	}
	if hasNamespace(b, SubscriberNamespace) {
		cfg, err := parseSubscriberConfig(b)
		if err != nil {
			return err
		}
		return validateSubscriber(cfg)
	}
	return nil
}

func hasNamespace(b *config.Backend, namespace string) bool {
	_, ok := b.ExtraConfig[namespace]
	return ok
}
