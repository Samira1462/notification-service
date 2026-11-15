package com.example.notifications.config;

import com.example.notifications.model.NotificationEvent;
import com.example.notifications.observability.NotificationMetrics;
import com.example.notifications.provider.NonRetryableNotificationException;
import com.example.notifications.service.NotificationTracker;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Retry and dead-letter policy for every listener. Spring Boot applies a single {@code CommonErrorHandler}
 * bean to the auto-configured listener container factory.
 *
 * <p>Flow for a failing record: retry with exponential backoff (retryable errors only) -> when retries
 * run out, or immediately for non-retryable errors and undeserialisable records -> mark the tracking
 * record FAILED and publish to the dead-letter topic -> commit the offset so the partition keeps moving.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, NotificationEvent> eventTemplate,
            KafkaTemplate<String, byte[]> bytesTemplate,
            NotificationProperties properties) {

        // Order matters: the first template whose key type matches the record value is used.
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);
        templates.put(NotificationEvent.class, eventTemplate);

        // Partition -1 lets the producer choose the partition from the record key.
        return new DeadLetterPublishingRecoverer(
                templates, (record, exception) -> new TopicPartition(properties.topics().dlq(), -1));
    }

    @Bean
    public DefaultErrorHandler notificationErrorHandler(
            DeadLetterPublishingRecoverer deadLetterRecoverer,
            NotificationTracker tracker,
            NotificationMetrics metrics,
            NotificationProperties properties) {

        NotificationProperties.Retry retry = properties.retry();
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxRetries());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());

        ConsumerRecordRecoverer recoverer = (record, exception) -> {
            String reason = rootCauseMessage(exception);
            if (record.value() instanceof NotificationEvent event) {
                if (event.eventId() != null) {
                    tracker.markFailed(event.eventId(), reason);
                }
                metrics.deadLettered(event.type());
                log.error("Dead-lettering event {} from {}: {}", event.eventId(), record.topic(), reason);
            } else {
                // Undeserialisable record: nothing to mark, but it still goes to the DLQ as raw bytes.
                metrics.deadLettered(null);
                log.error("Dead-lettering undeserialisable record {}-{}@{}: {}",
                        record.topic(), record.partition(), record.offset(), reason);
            }
            deadLetterRecoverer.accept(record, exception);
        };

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // Permanent failures skip the retries entirely.
        handler.addNotRetryableExceptions(NonRetryableNotificationException.class);
        // Commit the offset of a dead-lettered record so a restart does not reprocess it.
        handler.setCommitRecovered(true);
        return handler;
    }

    private static String rootCauseMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
