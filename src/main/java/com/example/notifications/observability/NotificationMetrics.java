package com.example.notifications.observability;

import com.example.notifications.domain.NotificationRecordRepository;
import com.example.notifications.model.NotificationStatus;
import com.example.notifications.model.NotificationType;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

/**
 * Micrometer metrics, scraped from {@code /actuator/prometheus}.
 *
 * <ul>
 *   <li>{@code notifications.processed{channel,outcome}} - sent / duplicate / retry
 *   <li>{@code notifications.dlq{channel}} - events dead-lettered
 *   <li>{@code notifications.provider.latency{channel}} - provider call duration
 *   <li>{@code notifications.e2e.latency{channel}} - event creation to delivery
 *   <li>{@code notifications.records{status}} - rows per status (FAILED approximates the DLQ backlog)
 * </ul>
 *
 * Consumer lag comes from the Kafka client metrics Spring Boot binds to Micrometer
 * ({@code kafka_consumer_fetch_manager_records_lag_max}).
 */
@Component
public class NotificationMetrics {

    private final MeterRegistry registry;

    public NotificationMetrics(MeterRegistry registry, NotificationRecordRepository repository) {
        this.registry = registry;
        for (NotificationStatus status : NotificationStatus.values()) {
            Gauge.builder("notifications.records", repository, r -> r.countByStatus(status))
                    .description("Notification records by status")
                    .tag("status", status.name())
                    .register(registry);
        }
    }

    public void sent(NotificationType type, Instant createdAt) {
        count("sent", type);
        Timer.builder("notifications.e2e.latency")
                .tag("channel", channel(type))
                .register(registry)
                .record(Duration.between(createdAt, Instant.now()));
    }

    public void duplicate(NotificationType type) {
        count("duplicate", type);
    }

    public void retry(NotificationType type) {
        count("retry", type);
    }

    public void deadLettered(NotificationType type) {
        registry.counter("notifications.dlq", "channel", channel(type)).increment();
    }

    public void providerLatency(NotificationType type, long nanos) {
        Timer.builder("notifications.provider.latency")
                .tag("channel", channel(type))
                .register(registry)
                .record(nanos, TimeUnit.NANOSECONDS);
    }

    private void count(String outcome, NotificationType type) {
        registry.counter("notifications.processed", "channel", channel(type), "outcome", outcome).increment();
    }

    private static String channel(NotificationType type) {
        return type == null ? "unknown" : type.name().toLowerCase();
    }
}
