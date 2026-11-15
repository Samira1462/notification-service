# Notification System with Spring Boot and Kafka

A runnable implementation of the design in Keyhole Software's
"Designing a Notification System at Scale with Spring Boot and Kafka": business services publish
notification events to Kafka, and channel workers (email, SMS, push) deliver them.

> **Status:** this project was written without access to Maven Central, so it has **not been compiled or
> run by its author**. Run `mvn verify` first; if anything fails, the fix should be small, and the tests
> below tell you which part misbehaves.

## Architecture

```
 Business services / REST API
        │  POST /api/notifications  (or NotificationPublisher.publish)
        ▼
  notification_record  ◄── PENDING row written first (tracking + client-retry dedupe)
        │
        ▼  key = userId (per-user ordering)
 ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
 │notifications. │ │notifications. │ │notifications. │      (or one shared topic)
 │     email     │ │      sms      │ │     push      │
 └──────┬────────┘ └──────┬────────┘ └──────┬────────┘
        ▼                 ▼                 ▼
   @KafkaListener workers, one consumer group, per-channel concurrency
        │
        ▼  NotificationService.process(event)
   dedupe (event id) → render template → rate-limit → provider.send(idempotencyKey) → mark SENT
        │
        ├─ retryable failure ──► exponential backoff, retry in place
        └─ non-retryable, or retries exhausted ──► notifications.dlq  (+ record marked FAILED)
```

## Article concept → code

| Article section | Where |
|---|---|
| Decouple business logic from providers | `producer/NotificationPublisher`, `web/NotificationController` |
| Single topic vs per-channel topics | `notification.topic-mode`; `consumer/SharedTopicListener` vs `consumer/ChannelTopicListeners` |
| Event model (id, user, type, recipient, template, data) | `model/NotificationEvent` |
| Template-based events | `service/TemplateRenderer` |
| Consumers routing by type | `service/NotificationService` + `provider/*` |
| Retries with exponential backoff | `config/KafkaErrorHandlingConfig` (`ExponentialBackOffWithMaxRetries`) |
| Retryable vs non-retryable failures | `provider/RetryableNotificationException`, `NonRetryableNotificationException` |
| Dead-letter queue and replay | `DeadLetterPublishingRecoverer`, `POST /api/notifications/{id}/replay` |
| Idempotency / duplicate detection | `service/NotificationTracker.begin` (unique `event_id`) + provider idempotency key |
| Notification tracking (PENDING/PROCESSING/SENT/FAILED) | `domain/NotificationRecord` |
| Horizontal scaling, partitions cap parallelism | consumer group, `notification.topics.partitions`, `notification.concurrency.*` |
| Rate limiting outbound calls | `service/ChannelRateLimiters` (Resilience4j) |
| Observability | `observability/NotificationMetrics`, Actuator + Micrometer + Prometheus + Grafana |

## Run it

Requirements: JDK 21+, Maven 3.9+, Docker.

```bash
docker compose up -d kafka            # add: postgres prometheus grafana  (optional)
mvn spring-boot:run                   # H2 in-memory tracking DB
# or with Postgres:  mvn spring-boot:run -Dspring-boot.run.profiles=postgres
```

Send notifications:

```bash
# email (returns 202; the eventId is your idempotency key, reuse it when retrying)
curl -s -X POST localhost:8080/api/notifications -H 'Content-Type: application/json' -d '{
  "userId": "u-1", "type": "EMAIL", "recipient": "ada@example.com",
  "templateId": "ORDER_CONFIRMATION",
  "data": {"name": "Ada", "orderId": "A-1001", "total": "$42.00"}
}'

# SMS and push
curl -s -X POST localhost:8080/api/notifications -H 'Content-Type: application/json' -d '{
  "userId": "u-1", "type": "SMS", "recipient": "+15551234567",
  "templateId": "PAYMENT_RECEIVED", "data": {"name": "Ada", "amount": "$42.00"}}'

# status, failures, replay
curl -s localhost:8080/api/notifications/<eventId>
curl -s 'localhost:8080/api/notifications?status=FAILED'
curl -s -X POST localhost:8080/api/notifications/<eventId>/replay
```

Templates: `ORDER_CONFIRMATION`, `PAYMENT_RECEIVED`, `SHIPPING_UPDATE`, `PASSWORD_RESET`.

Try the failure paths:

- Invalid recipient (e.g. SMS to `12345`) → non-retryable → straight to the DLQ, record `FAILED`.
- `--notification.providers.simulated-failure-rate=0.6` → watch retries with backoff in the logs and in
  `notifications_processed_total{outcome="retry"}`.
- Inspect the DLQ:
  `docker exec notifications-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic notifications.dlq --from-beginning --property print.headers=true`

Metrics: <http://localhost:8080/actuator/prometheus>, Prometheus <http://localhost:9090>, Grafana
<http://localhost:3000> (Prometheus data source pre-provisioned). Useful queries:
`kafka_consumer_fetch_manager_records_lag_max` (consumer lag), `notifications_records{status="FAILED"}`,
`rate(notifications_processed_total[1m])`, `notifications_provider_latency_seconds`.

## Tests

```bash
mvn verify
```

`NotificationFlowTest` runs the real pipeline against an embedded Kafka broker with a scripted provider:
single delivery, duplicate suppression, retry then success, non-retryable → DLQ, retries exhausted → DLQ →
replay → delivered, a poison-pill message that must not block the partition, and the REST API
(dedupe by event id, validation, 404).

## Design notes and caveats

- **At-least-once, not exactly-once.** A crash after the provider accepted a message but before `SENT`
  is committed causes a resend. The event id is passed to the provider as an idempotency key; wire it to
  your provider's mechanism (e.g. SES/Twilio/FCM request ids) to close that gap.
- **Two workers racing on one event** can both see it as unsent. The unique `event_id` prevents duplicate
  rows, not duplicate sends; the provider idempotency key is again the safeguard.
- **Retries block the partition.** Backoff happens inside the consumer thread, which is simple and keeps
  per-user order, but a long outage delays everything behind the failing record. Keep
  `retry.max-retries`/`max-interval` modest (the worst case must stay far below `max.poll.interval.ms`,
  default 5 min), or move to non-blocking retry topics (`@RetryableTopic`) if you need long delays.
- **Rate limits are per instance.** Divide the provider contract by the instance count, or back the
  limiter with a shared store like Redis.
- **Partitions cap parallelism.** More consumers than partitions sit idle, and changing partition count
  later re-maps keys (breaking per-user ordering across the change). Size `notification.topics.partitions`
  up front.
- **Dual write on ingest.** The PENDING row and the Kafka publish are not atomic. If publish fails the row
  is removed and the client gets 503; if the process dies in between, a client retry with the same
  `eventId` republishes because the row is still PENDING. For stricter guarantees use a transactional
  outbox.
- **Production hardening not included:** authentication on the API (it returns recipients), Flyway
  migrations instead of `ddl-auto: update`, real provider SDKs (replace `SimulatedProvider`), Kafka
  security/replication settings (`replicas: 1` is for local use), and PII-safe logging.
