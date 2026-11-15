package com.example.notifications.service;

import com.example.notifications.model.NotificationEvent;
import com.example.notifications.observability.NotificationMetrics;
import com.example.notifications.provider.NonRetryableNotificationException;
import com.example.notifications.provider.NotificationProvider;
import com.example.notifications.provider.ProviderRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Worker-side processing of one event: deduplicate, render, throttle, route to the channel provider and
 * record the outcome.
 *
 * <p>Failures are signalled by exceptions and handled by the Kafka error handler (see
 * {@code KafkaErrorHandlingConfig}): {@link NonRetryableNotificationException} goes straight to the
 * dead-letter topic, anything else is retried with exponential backoff first.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationTracker tracker;
    private final TemplateRenderer renderer;
    private final ChannelRateLimiters rateLimiters;
    private final ProviderRegistry providers;
    private final NotificationMetrics metrics;

    public NotificationService(
            NotificationTracker tracker,
            TemplateRenderer renderer,
            ChannelRateLimiters rateLimiters,
            ProviderRegistry providers,
            NotificationMetrics metrics) {
        this.tracker = tracker;
        this.renderer = renderer;
        this.rateLimiters = rateLimiters;
        this.providers = providers;
        this.metrics = metrics;
    }

    public void process(NotificationEvent event) {
        validate(event);

        if (!tracker.begin(event)) {
            log.info("Skipping duplicate event {} ({})", event.eventId(), event.type());
            metrics.duplicate(event.type());
            return;
        }

        try {
            TemplateRenderer.Rendered message = renderer.render(event.templateId(), event.data());
            rateLimiters.acquire(event.type());

            // Route by channel: the providers differ, the pipeline around them does not.
            NotificationProvider provider = providers.forChannel(event.type());
            long start = System.nanoTime();
            provider.send(event.eventId(), event.recipient(), message.subject(), message.body());
            metrics.providerLatency(event.type(), System.nanoTime() - start);

            // If this write fails after a successful send, the retry will send again; the idempotency
            // key passed to the provider is what protects against a visible duplicate.
            tracker.markSent(event.eventId());
            metrics.sent(event.type(), event.createdAt());
            log.debug("Event {} sent via {}", event.eventId(), event.type());
        } catch (NonRetryableNotificationException e) {
            // Permanent failure. The error handler marks it FAILED and dead-letters it without retrying.
            throw e;
        } catch (RuntimeException e) {
            tracker.markRetrying(event.eventId(), describe(e));
            metrics.retry(event.type());
            log.warn("Event {} failed, will retry: {}", event.eventId(), e.toString());
            throw e;
        }
    }

    private static void validate(NotificationEvent event) {
        if (event == null || event.eventId() == null || event.eventId().isBlank()) {
            throw new NonRetryableNotificationException("Event has no eventId");
        }
        if (event.type() == null) {
            throw new NonRetryableNotificationException("Event " + event.eventId() + " has no type");
        }
        // Reject malformed events up front: they can never succeed and would otherwise fail on a
        // database constraint and be retried pointlessly.
        if (isBlank(event.userId()) || isBlank(event.recipient()) || isBlank(event.templateId())) {
            throw new NonRetryableNotificationException(
                    "Event " + event.eventId() + " is missing userId, recipient or templateId");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static String describe(Throwable t) {
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
