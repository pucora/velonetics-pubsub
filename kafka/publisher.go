package kafka

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/segmentio/kafka-go"
	"github.com/velonetics/lura/v2/config"
	"github.com/velonetics/lura/v2/logging"
	"github.com/velonetics/lura/v2/proxy"
)

func initPublisher(
	ctx context.Context,
	logger logging.Logger,
	remote *config.Backend,
) (proxy.Proxy, error) {
	cfg, err := parsePublisherConfig(remote)
	if err != nil {
		return proxy.NoopProxy, err
	}
	if err := validatePublisher(cfg); err != nil {
		return proxy.NoopProxy, err
	}

	transport, err := newWriterTransport(cfg.Writer.Cluster)
	if err != nil {
		return proxy.NoopProxy, err
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Writer.Cluster.Brokers...),
		Topic:                  cfg.Writer.Topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           requiredAcks(cfg.Writer.Producer.RequiredAcks),
		Compression:            compression(cfg.Writer.Producer.CompressionCodec),
		MaxAttempts:            maxInt(cfg.Writer.Producer.RetryMax, 3),
		Transport:              transport,
	}
	if cfg.Writer.Producer.Idempotent {
		writer.RequiredAcks = kafka.RequireAll
	}

	logPrefix := fmt.Sprintf("[BACKEND: kafka://%s/%s][PubSub/Kafka]", cfg.Writer.Cluster.Brokers[0], cfg.Writer.Topic)
	logger.Debug(logPrefix, "Publisher initialized successfully")

	go func() {
		<-ctx.Done()
		_ = writer.Close()
	}()

	statusCode := cfg.SuccessStatusCode
	if statusCode == 0 {
		statusCode = 200
	}

	return func(ctx context.Context, r *proxy.Request) (*proxy.Response, error) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}

		msg := kafka.Message{Value: body}
		if cfg.Writer.KeyMeta != "" {
			if values, ok := r.Headers[cfg.Writer.KeyMeta]; ok && len(values) > 0 {
				msg.Key = []byte(values[0])
			}
		}

		if err := writer.WriteMessages(ctx, msg); err != nil {
			return nil, err
		}

		return &proxy.Response{
			IsComplete: true,
			Metadata: proxy.Metadata{
				StatusCode: statusCode,
			},
		}, nil
	}, nil
}

func maxInt(v, fallback int) int {
	if v <= 0 {
		return fallback
	}
	return v
}
