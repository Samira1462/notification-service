package com.example.notifications.config;

import com.example.notifications.model.NotificationType;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Typed view of the {@code notification.*} configuration block. */
@ConfigurationProperties(prefix = "notification")
public record NotificationProperties(
        TopicMode topicMode,
        Topics topics,
        Map<NotificationType, Integer> concurrency,
        int sharedConcurrency,
        Duration publishTimeout,
        Retry retry,
        Map<NotificationType, RateLimit> rateLimit,
        Providers providers) {

    public enum TopicMode {
        /** One topic for all events; the worker routes by {@link NotificationType}. */
        SHARED,
        /** One topic per channel, each scaled and configured independently. */
        PER_CHANNEL
    }

    public record Topics(
            String shared, String email, String sms, String push, String dlq, int partitions, int replicas) {

        public String forType(NotificationType type) {
            return switch (type) {
                case EMAIL -> email;
                case SMS -> sms;
                case PUSH -> push;
            };
        }
    }

    public record Retry(int maxRetries, Duration initialInterval, double multiplier, Duration maxInterval) {}

    public record RateLimit(int limitForPeriod, Duration refreshPeriod, Duration timeout) {}

    public record Providers(double simulatedFailureRate, Duration simulatedLatency) {}

    /** Topic a producer must publish an event of the given type to, honouring the topic mode. */
    public String topicFor(NotificationType type) {
        return topicMode == TopicMode.SHARED ? topics.shared() : topics.forType(type);
    }
}
