# velonetics-pubsub
a pubsub backend for the Velonetics framework

## Backends

- AWS SNS (Simple Notification Service) and SQS (Simple Queueing Service)
- Azure Service Bus Topic and Subscription
- GCP PubSub
- NATS.io
- RabbitMQ 

## Configuration

Just add the extra config at your backend:

```
"github.com/velonetics/velonetics-pubsub/subscriber": {
	"subscription_url": "gcppubsub://project/topic"
}
```
```
"github.com/velonetics/velonetics-pubsub/publisher": {
	"topic_url": "gcppubsub://project/topic"
}
```
