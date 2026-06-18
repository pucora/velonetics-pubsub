# velonetics-pubsub

Pub/Sub backend for the Velonetics API gateway. Connects HTTP endpoints to message brokers using [Go Cloud Pub/Sub](https://gocloud.dev/howto/pubsub/).

## Supported drivers

| Driver | `host` scheme | Environment variable |
|--------|---------------|----------------------|
| Apache Kafka | `kafka://` | `KAFKA_BROKERS` |
| NATS.io | `nats://` | `NATS_SERVER_URL` |
| RabbitMQ | `rabbit://` | `RABBIT_SERVER_URL` |
| GCP Pub/Sub | `gcppubsub://` | `GOOGLE_APPLICATION_CREDENTIALS` |
| AWS SNS | `awssns:///` + ARN in host | AWS credentials |
| AWS SQS | `awssqs://` + queue URL | AWS credentials |
| Azure Service Bus | `azuresb://` | `SERVICEBUS_CONNECTION_STRING` |

## Configuration

Set the broker scheme in `host[0]` and the topic or subscription path in `extra_config`. `disable_host_sanitize: true` is required for non-HTTP schemes. `url_pattern` is required by schema but unused — set any value.

### Publisher (HTTP → broker)

```json
{
  "host": ["kafka://"],
  "url_pattern": "/ignored",
  "disable_host_sanitize": true,
  "extra_config": {
    "backend/pubsub/publisher": {
      "topic_url": "mytopic"
    }
  }
}
```

### Subscriber (broker → HTTP response)

```json
{
  "host": ["gcppubsub://"],
  "url_pattern": "/ignored",
  "disable_host_sanitize": true,
  "extra_config": {
    "backend/pubsub/subscriber": {
      "subscription_url": "myproject/mysub"
    }
  }
}
```

On publish, the HTTP request body is sent as the message body and the first value of each HTTP header is copied to message metadata. On subscribe, one message is pulled per HTTP request, decoded with the backend encoding (typically JSON), and returned as the response.

## Kafka advanced and async agents

For mTLS/SASL Kafka connections use `backend/pubsub/publisher/kafka` and `backend/pubsub/subscriber/kafka`. For background consumption without HTTP clients use the `async/kafka` driver in `async_agent` configuration.

## Recent releases

- **v2.0.5** — Kafka async pending-offset retry, config-only startup probe, HTTP subscriber pending commits, format-before-commit/ack
- See [non-REST bugfix changelog](https://github.com/velonetics/velonetics-ce/blob/main/docs/non-rest-connectivity/BUGFIX-CHANGELOG.md) in velonetics-ce for full cross-module history
