package com.example.notifications;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.notifications.config.NotificationProperties;
import com.example.notifications.domain.NotificationRecord;
import com.example.notifications.domain.NotificationRecordRepository;
import com.example.notifications.model.NotificationEvent;
import com.example.notifications.model.NotificationStatus;
import com.example.notifications.model.NotificationType;
import com.example.notifications.producer.NotificationPublisher;
import com.example.notifications.provider.NotificationProvider;
import com.example.notifications.provider.ProviderRegistry;
import com.example.notifications.provider.RetryableNotificationException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * End-to-end tests against real Kafka and PostgreSQL containers (Testcontainers, so Docker must be
 * running), using the same images as {@code docker-compose.yml}. The providers are replaced by a scripted one so failures are deterministic.
 */
@SpringBootTest(
        properties = {
            "notification.topics.partitions=1",
            "notification.retry.max-retries=3",
            "notification.retry.initial-interval=50ms",
            "notification.retry.max-interval=200ms"
        })
@AutoConfigureMockMvc
@DirtiesContext
@Testcontainers
class NotificationFlowIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    static final ScriptedProvider PROVIDER = new ScriptedProvider();

    @TestConfiguration
    static class ScriptedProviders {
        @Bean
        @Primary
        ProviderRegistry scriptedProviderRegistry() {
            return type -> PROVIDER;
        }
    }

    @Autowired NotificationPublisher publisher;
    @Autowired NotificationRecordRepository repository;
    @Autowired NotificationProperties properties;
    @Autowired MockMvc mvc;

    @BeforeEach
    void resetProvider() {
        PROVIDER.reset();
    }

    @Test
    void deliversAnEmailExactlyOnce() throws Exception {
        NotificationEvent event = orderEmail();

        publish(event);

        awaitStatus(event.eventId(), NotificationStatus.SENT);
        assertThat(PROVIDER.delivered(event.eventId())).isEqualTo(1);
    }

    @Test
    void redeliveredDuplicateIsNotSentTwice() throws Exception {
        NotificationEvent original = orderEmail();
        NotificationEvent sentinel = orderEmail();
        // Same userId means same partition key, so the order below is the processing order.
        NotificationEvent sentinelSameUser = new NotificationEvent(
                sentinel.eventId(), original.userId(), sentinel.type(), sentinel.recipient(),
                sentinel.templateId(), sentinel.data(), sentinel.createdAt());

        publish(original);
        publish(original); // simulates Kafka redelivery / a producer retry
        publish(sentinelSameUser);

        awaitStatus(sentinelSameUser.eventId(), NotificationStatus.SENT);
        awaitStatus(original.eventId(), NotificationStatus.SENT);
        assertThat(PROVIDER.delivered(original.eventId())).isEqualTo(1);
    }

    @Test
    void retriesTemporaryFailuresWithBackoffThenSucceeds() throws Exception {
        NotificationEvent event = orderEmail();
        PROVIDER.failNext(event.eventId(), 2);

        publish(event);

        awaitStatus(event.eventId(), NotificationStatus.SENT);
        NotificationRecord record = repository.findByEventId(event.eventId()).orElseThrow();
        assertThat(record.getRetryCount()).isEqualTo(2);
        assertThat(PROVIDER.attempts(event.eventId())).isEqualTo(3);
        assertThat(PROVIDER.delivered(event.eventId())).isEqualTo(1);
    }

    @Test
    void nonRetryableFailureGoesStraightToTheDeadLetterTopic() throws Exception {
        NotificationEvent event = NotificationEvent.of(
                "user-" + UUID.randomUUID(), NotificationType.EMAIL, "ada@example.com", "NO_SUCH_TEMPLATE", Map.of());

        publish(event);

        awaitStatus(event.eventId(), NotificationStatus.FAILED);
        NotificationRecord record = repository.findByEventId(event.eventId()).orElseThrow();
        assertThat(record.getRetryCount()).isZero();
        assertThat(record.getLastError()).contains("Unknown template");
        assertThat(PROVIDER.attempts(event.eventId())).isZero();

        ConsumerRecord<String, String> dead = awaitDeadLetter(event.eventId());
        assertThat(dead.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
    }

    @Test
    void exhaustedRetriesAreDeadLetteredAndCanBeReplayed() throws Exception {
        NotificationEvent event = orderEmail();
        PROVIDER.failNext(event.eventId(), 1_000); // provider is "down"

        publish(event);

        awaitStatus(event.eventId(), NotificationStatus.FAILED);
        NotificationRecord failed = repository.findByEventId(event.eventId()).orElseThrow();
        assertThat(failed.getRetryCount()).isEqualTo(properties.retry().maxRetries() + 1);
        awaitDeadLetter(event.eventId());

        PROVIDER.failNext(event.eventId(), 0); // provider recovered
        mvc.perform(post("/api/notifications/{id}/replay", event.eventId())).andExpect(status().isAccepted());

        awaitStatus(event.eventId(), NotificationStatus.SENT);
        assertThat(PROVIDER.delivered(event.eventId())).isEqualTo(1);
    }

    @Test
    void poisonPillGoesToTheDeadLetterTopicAndDoesNotBlockTheQueue() throws Exception {
        String garbage = "{ this is not json " + UUID.randomUUID();
        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(KAFKA.getBootstrapServers()), new StringSerializer(), new StringSerializer());
        try {
            new KafkaTemplate<>(factory)
                    .send(properties.topics().email(), "poison-user", garbage)
                    .get(10, TimeUnit.SECONDS);
        } finally {
            factory.destroy();
        }
        NotificationEvent after = orderEmail();
        publish(after);

        awaitDeadLetter(garbage);
        awaitStatus(after.eventId(), NotificationStatus.SENT);
    }

    @Test
    void apiAcceptsThenDeduplicatesByEventId() throws Exception {
        String eventId = UUID.randomUUID().toString();
        String json = """
                {"eventId":"%s","userId":"u-api","type":"EMAIL","recipient":"ada@example.com",
                 "templateId":"PAYMENT_RECEIVED","data":{"name":"Ada","amount":"$10"}}
                """.formatted(eventId);

        mvc.perform(post("/api/notifications").contentType(MediaType.APPLICATION_JSON).content(json))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value(eventId))
                .andExpect(jsonPath("$.duplicate").value(false));
        awaitStatus(eventId, NotificationStatus.SENT);

        mvc.perform(post("/api/notifications").contentType(MediaType.APPLICATION_JSON).content(json))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true));

        mvc.perform(get("/api/notifications/{id}", eventId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SENT"));
        assertThat(PROVIDER.delivered(eventId)).isEqualTo(1);
    }

    @Test
    void apiRejectsInvalidRequests() throws Exception {
        mvc.perform(post("/api/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":\"u\",\"type\":\"EMAIL\",\"templateId\":\"PAYMENT_RECEIVED\"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/notifications/{id}", "does-not-exist")).andExpect(status().isNotFound());
    }

    // ---- helpers -----------------------------------------------------------------------------

    private static NotificationEvent orderEmail() {
        return NotificationEvent.of(
                "user-" + UUID.randomUUID(),
                NotificationType.EMAIL,
                "ada@example.com",
                "ORDER_CONFIRMATION",
                Map.of("name", "Ada", "orderId", "A-1", "total", "$9.99"));
    }

    private void publish(NotificationEvent event) throws Exception {
        publisher.publish(event).get(10, TimeUnit.SECONDS);
    }

    private void awaitStatus(String eventId, NotificationStatus expected) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(repository.findByEventId(eventId))
                .get()
                .extracting(NotificationRecord::getStatus)
                .isEqualTo(expected));
    }

    /** Waits for a record on the DLQ whose value contains {@code needle} (an event id or raw payload). */
    private ConsumerRecord<String, String> awaitDeadLetter(String needle) {
        Map<String, Object> config = KafkaTestUtils.consumerProps(KAFKA.getBootstrapServers(), "dlq-test-" + UUID.randomUUID(), "true");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, String> consumer =
                new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new StringDeserializer())
                        .createConsumer()) {
            consumer.subscribe(List.of(properties.topics().dlq()));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.value() != null && record.value().contains(needle)) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("No dead-letter record containing " + needle);
    }

    /** Provider whose failures are scripted per event id. */
    static class ScriptedProvider implements NotificationProvider {

        private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> delivered = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> failuresRemaining = new ConcurrentHashMap<>();

        @Override
        public NotificationType channel() {
            return NotificationType.EMAIL;
        }

        @Override
        public void send(String idempotencyKey, String recipient, String subject, String body) {
            counter(attempts, idempotencyKey).incrementAndGet();
            AtomicInteger remaining = failuresRemaining.get(idempotencyKey);
            if (remaining != null && remaining.getAndDecrement() > 0) {
                throw new RetryableNotificationException("scripted provider timeout");
            }
            counter(delivered, idempotencyKey).incrementAndGet();
        }

        void failNext(String eventId, int times) {
            failuresRemaining.put(eventId, new AtomicInteger(times));
        }

        int attempts(String eventId) {
            return counter(attempts, eventId).get();
        }

        int delivered(String eventId) {
            return counter(delivered, eventId).get();
        }

        void reset() {
            attempts.clear();
            delivered.clear();
            failuresRemaining.clear();
        }

        private static AtomicInteger counter(Map<String, AtomicInteger> map, String key) {
            return map.computeIfAbsent(key, k -> new AtomicInteger());
        }
    }
}
