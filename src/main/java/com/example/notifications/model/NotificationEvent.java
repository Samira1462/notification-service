package com.example.notifications.model;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The message business services publish to Kafka.
 *
 * <p>It references a template instead of carrying rendered content, which keeps events small and lets
 * wording change without touching producers. {@code eventId} is the idempotency key used for duplicate
 * detection, so a producer that retries must reuse it.
 */
public record NotificationEvent(
        String eventId,
        String userId,
        NotificationType type,
        String recipient,
        String templateId,
        Map<String, String> data,
        Instant createdAt) {

    public NotificationEvent {
        data = data == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(data));
        createdAt = createdAt == null ? Instant.now() : createdAt;
    }

    public static NotificationEvent of(
            String userId, NotificationType type, String recipient, String templateId, Map<String, String> data) {
        return new NotificationEvent(
                UUID.randomUUID().toString(), userId, type, recipient, templateId, data, Instant.now());
    }
}
