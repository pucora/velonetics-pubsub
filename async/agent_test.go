package async

import (
	"context"
	"testing"
	"time"

	kafkapkg "github.com/pucora/pucora-pubsub/v2/kafka"
	"github.com/pucora/lura/v2/async"
	"github.com/pucora/lura/v2/config"
	"github.com/pucora/lura/v2/logging"
	"golang.org/x/sync/errgroup"
)

func TestStartAgent_noConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)
	started := StartAgent(gctx, async.Options{
		Agent: &config.AsyncAgent{
			Name: "kafka-agent",
			Consumer: config.Consumer{
				Topic:   "events",
				Workers: 1,
				Timeout: time.Second,
			},
			Connection: config.Connection{
				HealthInterval: time.Second,
			},
			ExtraConfig: config.ExtraConfig{},
		},
		G:              g,
		ShouldContinue: func(int) bool { return false },
		BackoffF:       func(int) time.Duration { return 0 },
		Logger:         logging.NoOp,
	})

	if started {
		t.Fatal("expected StartAgent to return false without async/kafka config")
	}
}

func TestStartAgent_invalidConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)
	started := StartAgent(gctx, async.Options{
		Agent: &config.AsyncAgent{
			Name: "kafka-agent",
			Consumer: config.Consumer{
				Topic:   "events",
				Workers: 1,
				Timeout: time.Second,
			},
			Connection: config.Connection{
				HealthInterval: time.Second,
			},
			ExtraConfig: config.ExtraConfig{
				kafkapkg.AsyncDriverNamespace: map[string]interface{}{
					"cluster": map[string]interface{}{},
				},
			},
		},
		G:              g,
		ShouldContinue: func(int) bool { return false },
		BackoffF:       func(int) time.Duration { return 0 },
		Logger:         logging.NoOp,
	})

	if !started {
		t.Fatal("expected StartAgent to return true when async/kafka namespace is present")
	}
}
