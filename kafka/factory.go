package kafka

import (
	"context"

	"github.com/velonetics/lura/v2/config"
	"github.com/velonetics/lura/v2/logging"
	"github.com/velonetics/lura/v2/proxy"
)

// ErrorProxy returns a proxy that always fails with the given initialization error.
func ErrorProxy(err error) proxy.Proxy {
	return func(ctx context.Context, _ *proxy.Request) (*proxy.Response, error) {
		return nil, err
	}
}

func TryInitSubscriber(ctx context.Context, logger logging.Logger, remote *config.Backend) (proxy.Proxy, error) {
	prxy, err := initSubscriber(ctx, logger, remote)
	if err != nil && !IsNamespaceNotFound(err) {
		logger.Error("[BACKEND][PubSub/Kafka] Error initializing subscriber:", err.Error())
	}
	return prxy, err
}

func TryInitPublisher(ctx context.Context, logger logging.Logger, remote *config.Backend) (proxy.Proxy, error) {
	prxy, err := initPublisher(ctx, logger, remote)
	if err != nil && !IsNamespaceNotFound(err) {
		logger.Error("[BACKEND][PubSub/Kafka] Error initializing publisher:", err.Error())
	}
	return prxy, err
}
