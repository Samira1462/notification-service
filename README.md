# Notification Service

A backend service that sends **email, SMS and push notifications** reliably, built with
**Java 21, Spring Boot and Apache Kafka**.

Other services (orders, payments, login, …) don't talk to email or SMS providers themselves. They hand
the notification to this service, which queues it in Kafka, delivers it through the right channel,
retries when a provider has a temporary problem, and keeps failed notifications aside so they can be
inspected and sent again later.

```text
"Order A-1001 confirmed"  ──►  POST /api/notifications  ──►  Kafka  ──►  worker  ──►  email provider
                                     202 Accepted                         │
                                                                          ├─ temporary error → retry with backoff
                                                                          └─ permanent error → dead-letter topic
```

## Features

- **Three channels:** email, SMS and push, each on its own Kafka topic with its own workers, so a slow
  SMS provider can't delay emails.
- **Templates:** events carry a template id plus data (`{"name": "Ada", "orderId": "A-1001"}`); the
  wording lives in one place.
- **Automatic retries:** temporary failures (timeouts, provider outages) are retried with exponential
  backoff (0.5s, 1s, 2s, 4s).
- **Dead-letter queue (DLQ):** permanent failures (e.g. an invalid phone number) and messages that keep
  failing are moved to a separate topic instead of blocking the queue, and can be **replayed** through
  the API.
- **No double sends on retries:** every notification has an `eventId`; the same id is never delivered
  twice by this service.
- **Status tracking:** every notification is stored in a database with its status
  (`PENDING → PROCESSING → SENT / FAILED`), retry count and last error.
- **Rate limiting:** outgoing calls are throttled per channel to stay within provider limits.
- **Per-user ordering:** a user's notifications are delivered in the order they were sent.
- **Monitoring:** Prometheus metrics and a Grafana instance, ready to use.

## Tech stack

| | |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3.5 (Web, Validation, Data JPA, Actuator) |
| Messaging | Apache Kafka 3.9, Spring for Apache Kafka |
| Database | PostgreSQL 16 |
| Rate limiting | Resilience4j |
| Metrics | Micrometer, Prometheus, Grafana |
| Tests | JUnit 5, Testcontainers (Kafka, PostgreSQL), MockMvc |
| Build | Maven |

## Quick start

Requirements: **JDK 21+**, **Maven 3.9+**, **Docker**.

```bash
docker compose up -d kafka postgres   # start Kafka and PostgreSQL
mvn spring-boot:run                   # start the service on http://localhost:8080
```

Send a notification:

```bash
curl -X POST localhost:8080/api/notifications -H 'Content-Type: application/json' -d '{
  "eventId": "order-A-1001",
  "userId": "u-1",
  "type": "EMAIL",
  "recipient": "ada@example.com",
  "templateId": "ORDER_CONFIRMATION",
  "data": {"name": "Ada", "orderId": "A-1001", "total": "$42.00"}
}'
```

```json
{"eventId":"order-A-1001","topic":"notifications.email","duplicate":false}
```

Check its status a moment later:

```bash
curl localhost:8080/api/notifications/order-A-1001
```

```json
{"eventId":"order-A-1001","userId":"u-1","channel":"EMAIL","recipient":"ada@example.com",
 "status":"SENT","retryCount":0,"lastError":null,
 "createdAt":"2026-11-15T18:54:44.549Z","updatedAt":"2026-11-15T18:54:44.763Z"}
```

> The providers are **simulated**: nothing is really emailed or texted. A delivery shows up as a
> `Delivered EMAIL notification` line in the application log. See
> [Plugging in real providers](#plugging-in-real-providers).

## How it works

```text
 Business services / REST API
        │  POST /api/notifications
        ▼
 ① save record as PENDING ──► notification_record table (PostgreSQL)
        │
        ▼  ② publish to Kafka, key = userId
 ┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────┐
 │ notifications.email │ │  notifications.sms  │ │ notifications.push  │
 └──────────┬──────────┘ └──────────┬──────────┘ └──────────┬──────────┘
            ▼                       ▼                       ▼
 ③ workers (one consumer group, several threads per channel)
          │
          ▼  NotificationService.process(event)
   skip if already handled → render template → wait for rate limit → call provider → mark SENT
          │
          ├─ temporary error ──► retry in place with exponential backoff
          └─ permanent error, or retries used up ──► notifications.dlq  + record marked FAILED
                                                              │
                                    POST /api/notifications/{eventId}/replay  ◄┘  (after fixing the cause)
```

1. **Accept.** The API validates the request and stores a `PENDING` record. If the same `eventId` was
   already delivered or failed, it answers `200` with `"duplicate": true` and publishes nothing.
2. **Queue.** The event is published to the topic of its channel, using the `userId` as the Kafka key.
   Kafka keeps all events with the same key in one partition, which preserves per-user order. The API
   returns `202 Accepted` only after Kafka has confirmed the write.
3. **Deliver.** A worker picks up the event, checks the database so it doesn't deliver the same
   `eventId` twice, fills in the template, takes a rate-limit permit and calls the provider. The
   `eventId` is passed to the provider as an *idempotency key* (see
   [Delivery guarantees](#delivery-guarantees-and-limitations)).
4. **Handle failures.**
   - *Temporary* (`RetryableNotificationException`, or any unexpected error): retried up to 4 times
     with growing pauses. The record goes back to `PENDING` and `retryCount` goes up.
   - *Permanent* (`NonRetryableNotificationException`: unknown template, missing template variable,
     invalid recipient): no retry.
   - Permanent failures, events that ran out of retries, and messages that aren't valid JSON go to
     `notifications.dlq`. The record is marked `FAILED` with the reason in `lastError`, and the worker
     moves on to the next message.
5. **Replay.** Once the cause is fixed (provider back online, template corrected), `POST
   /api/notifications/{eventId}/replay` publishes the original event again.

The service can also run with **one shared topic** for all channels (`notification.topic-mode: SHARED`)
instead of one topic per channel. A single worker then routes each event by its type.

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/notifications` | Send a notification |
| `GET` | `/api/notifications/{eventId}` | Status of one notification |
| `GET` | `/api/notifications?status=FAILED&limit=50` | List notifications by status (newest first, max 500) |
| `POST` | `/api/notifications/{eventId}/replay` | Send a `FAILED` notification again |

### Send request

| Field | Required | Description |
|---|---|---|
| `eventId` | no | Your unique id for this notification, max 100 chars. **Reuse it when you retry a request.** Generated if omitted. |
| `userId` | yes | The user the notification is for. |
| `type` | yes | `EMAIL`, `SMS` or `PUSH`. |
| `recipient` | yes | Email address, phone number in E.164 format (`+15551234567`), or push device token (8+ characters). |
| `templateId` | yes | One of the [templates](#templates). |
| `data` | no | Values for the template placeholders (max 50 entries). |

### Responses

| Status | When |
|---|---|
| `202 Accepted` | The notification was queued. |
| `200 OK`, `"duplicate": true` | This `eventId` was already delivered or failed; nothing new was sent. |
| `400 Bad Request` | A required field is missing or too long. |
| `404 Not Found` | Unknown `eventId`. |
| `409 Conflict` | Replay of a notification that isn't `FAILED`. |
| `503 Service Unavailable` | Kafka didn't confirm the write in time (10s). Nothing was queued; retry with the same `eventId`. |

`404`, `409` and `503` errors use the standard [Problem Details](https://www.rfc-editor.org/rfc/rfc9457) format:

```json
{"type":"about:blank","title":"Conflict","status":409,
 "detail":"Only FAILED notifications can be replayed; order-A-1001 is SENT",
 "instance":"/api/notifications/order-A-1001/replay"}
```

### Templates

| Template id | Placeholders |
|---|---|
| `ORDER_CONFIRMATION` | `name`, `orderId`, `total` |
| `PAYMENT_RECEIVED` | `name`, `amount` |
| `SHIPPING_UPDATE` | `name`, `orderId`, `trackingNumber` |
| `PASSWORD_RESET` | `name`, `code` |

Templates are defined in `service/TemplateRenderer.java`. A missing placeholder value is a permanent
failure.

## Try the failure paths

**Permanent failure.** An invalid phone number goes straight to the DLQ:

```bash
curl -X POST localhost:8080/api/notifications -H 'Content-Type: application/json' -d '{
  "eventId": "sms-bad-1", "userId": "u-1", "type": "SMS", "recipient": "12345",
  "templateId": "PAYMENT_RECEIVED", "data": {"name": "Ada", "amount": "$42.00"}}'

curl 'localhost:8080/api/notifications?status=FAILED'
# [{"eventId":"sms-bad-1", ..., "status":"FAILED",
#   "lastError":"NonRetryableNotificationException: Invalid phone number (expected E.164)", ...}]
```

**Temporary failures.** Make 60% of provider calls fail and watch the retries in the log:

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--notification.providers.simulated-failure-rate=0.6
```

**Read the DLQ** directly from Kafka:

```bash
docker exec notifications-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic notifications.dlq --from-beginning --property print.headers=true
```

## Project structure

```text
src/main/java/com/example/notifications/
├── web/            REST API: controller, request/response records, error handling
├── producer/       NotificationPublisher: writes events to Kafka (what other services would call)
├── consumer/       Kafka listeners: one per channel topic, or one for the shared topic
├── service/        Processing pipeline, status tracking, templates, rate limiting
├── provider/       Email / SMS / push providers (simulated) and the retryable/permanent exceptions
├── domain/         NotificationRecord entity and its repository
├── model/          NotificationEvent (the Kafka message), NotificationType, NotificationStatus
├── config/         Kafka topics, producers, retry and dead-letter handling, typed properties
└── observability/  Micrometer metrics
```

## Configuration

All settings are in `src/main/resources/application.yml` under `notification:` and can be overridden
with command-line arguments or environment variables.

| Setting | Default | Meaning |
|---|---|---|
| `topic-mode` | `PER_CHANNEL` | `PER_CHANNEL` (one topic per channel) or `SHARED` (one topic for all) |
| `topics.partitions` | `6` | Partitions per topic. Limits how many workers can process a channel in parallel. |
| `concurrency.email / sms / push` | `3 / 2 / 3` | Worker threads per channel on each instance |
| `retry.max-retries` | `4` | Retries before an event goes to the DLQ |
| `retry.initial-interval`, `multiplier`, `max-interval` | `500ms`, `2.0`, `10s` | Backoff between retries |
| `rate-limit.<channel>.limit-for-period` | email `50`, sms `10`, push `100` per second | Outgoing calls allowed per instance |
| `publish-timeout` | `10s` | How long the API waits for Kafka before answering `503` |
| `providers.simulated-failure-rate` | `0.0` | Share of simulated provider calls that fail temporarily (0 to 1) |
| `KAFKA_BOOTSTRAP_SERVERS` (env) | `localhost:9092` | Kafka address |
| `DB_URL` (env) | `jdbc:postgresql://localhost:5432/notifications` | PostgreSQL connection URL |
| `DB_USERNAME`, `DB_PASSWORD` (env) | `notifications` / `notifications` | PostgreSQL credentials (match `docker-compose.yml`) |

## Monitoring

```bash
docker compose up -d prometheus grafana
```

- Metrics endpoint: <http://localhost:8080/actuator/prometheus>
- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000> (no login needed; Prometheus is already added as a data source)

| Metric | What it shows |
|---|---|
| `notifications_processed_total{channel, outcome}` | Events handled, by outcome: `sent`, `duplicate`, `retry` |
| `notifications_dlq_total{channel}` | Events moved to the dead-letter topic |
| `notifications_records{status}` | Number of records in each status |
| `notifications_provider_latency_seconds` | Time spent calling the provider |
| `notifications_e2e_latency_seconds` | Time from event creation to delivery |
| `kafka_consumer_fetch_manager_records_lag_max` | How far the workers are behind (consumer lag) |

## Tests

```bash
mvn verify
```

- `TemplateRendererTest` covers template rendering and its errors.
- `NotificationFlowTest` runs the whole pipeline against real Kafka 3.9.1 and PostgreSQL 16
  containers started by Testcontainers (**Docker must be running**; the same images as
  `docker-compose.yml`), with a scripted provider. It covers a normal delivery, a redelivered duplicate, retry then success,
  a permanent failure going to the DLQ, retries running out followed by a successful replay, an invalid
  message that must not block the queue, and the REST API (duplicate handling, validation, 404).

## Delivery guarantees and limitations

- **At-least-once delivery.** If the service crashes after the provider accepted a message but before
  it was marked `SENT`, the message is sent again after restart. The same applies when two workers pick
  up the same event at the same time. To prevent a visible duplicate, the `eventId` is passed to the
  provider as an idempotency key; real providers (SES, Twilio, FCM, …) need to be wired to use it.
- **Repeating a request before it is delivered.** If a client repeats a request while the first one is
  still `PENDING`, the API answers `202` again and publishes it once more. The worker recognizes the
  `eventId` and skips the second copy (counted as `outcome="duplicate"`), so it isn't sent twice.
- **Retries pause the partition.** The retry wait happens inside the worker thread. This keeps per-user
  order and is simple, but other events in the same partition wait during a long outage. Keep the
  total retry time well below Kafka's `max.poll.interval.ms` (5 minutes by default), or switch to
  separate retry topics (`@RetryableTopic`) for longer delays.
- **Rate limits are per instance.** With several instances running, divide the provider's limit by the
  number of instances, or use a shared limiter (for example backed by Redis).
- **Choose the partition count up front.** More workers than partitions sit idle, and changing the
  count later moves users to different partitions, which breaks per-user order during the change.
- **Database and Kafka are not updated atomically.** If publishing fails, the `PENDING` record is
  removed and the client gets `503`. If the service dies between the two steps, a client retry with the
  same `eventId` publishes it. For a strict guarantee, use the transactional outbox pattern.

## Plugging in real providers

`EmailProvider`, `SmsProvider` and `PushProvider` extend `SimulatedProvider`, which checks the
recipient format, waits a few milliseconds and can fail on purpose. To go live, implement
`NotificationProvider` with a real SDK call and throw:

- `RetryableNotificationException` for temporary problems (timeouts, HTTP 429/5xx),
- `NonRetryableNotificationException` for problems a retry can't fix (invalid recipient, rejected
  content).

## Not production-ready yet

- No authentication on the API (it returns recipients' contact details).
- The database schema is created by Hibernate (`ddl-auto: update`); use Flyway or Liquibase migrations
  instead.
- Kafka runs with a single broker and no replication or security settings.
- Logs should be checked so they never contain personal data.

## Credits

The design follows the article *"Designing a Notification System at Scale with Spring Boot and Kafka"*
by Keyhole Software.
