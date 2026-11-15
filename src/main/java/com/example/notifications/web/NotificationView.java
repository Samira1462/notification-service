package com.example.notifications.web;

import com.example.notifications.domain.NotificationRecord;
import com.example.notifications.model.NotificationStatus;
import com.example.notifications.model.NotificationType;
import java.time.Instant;

/** Support-facing view of a tracking record. Contains the recipient, so protect these endpoints. */
public record NotificationView(
        String eventId,
        String userId,
        NotificationType channel,
        String recipient,
        NotificationStatus status,
        int retryCount,
        String lastError,
        Instant createdAt,
        Instant updatedAt) {

    public static NotificationView from(NotificationRecord r) {
        return new NotificationView(
                r.getEventId(),
                r.getUserId(),
                r.getChannel(),
                r.getRecipient(),
                r.getStatus(),
                r.getRetryCount(),
                r.getLastError(),
                r.getCreatedAt(),
                r.getUpdatedAt());
    }
}
