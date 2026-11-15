package com.example.notifications.domain;

import com.example.notifications.model.NotificationEvent;
import com.example.notifications.model.NotificationStatus;
import com.example.notifications.model.NotificationType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Persisted delivery state for one event. The unique {@code event_id} is what makes duplicate detection
 * safe when several workers race on the same redelivered event.
 */
@Entity
@Table(name = "notification_record")
public class NotificationRecord {

    private static final int MAX_ERROR_LENGTH = 1000;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true, updatable = false, length = 100)
    private String eventId;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private NotificationType channel;

    @Column(nullable = false)
    private String recipient;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private NotificationStatus status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "last_error", length = MAX_ERROR_LENGTH)
    private String lastError;

    /** Original event as JSON, so a FAILED notification can be replayed. */
    @Column(nullable = false, length = 8000)
    private String payload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected NotificationRecord() {}

    public static NotificationRecord create(NotificationEvent event, String payloadJson, NotificationStatus status) {
        NotificationRecord r = new NotificationRecord();
        r.eventId = event.eventId();
        r.userId = event.userId();
        r.channel = event.type();
        r.recipient = event.recipient();
        r.status = status;
        r.payload = payloadJson;
        return r;
    }

    @PrePersist
    void onCreate() {
        Instant now = Instant.now();
        createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getUserId() {
        return userId;
    }

    public NotificationType getChannel() {
        return channel;
    }

    public String getRecipient() {
        return recipient;
    }

    public NotificationStatus getStatus() {
        return status;
    }

    public void setStatus(NotificationStatus status) {
        this.status = status;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError == null || lastError.length() <= MAX_ERROR_LENGTH
                ? lastError
                : lastError.substring(0, MAX_ERROR_LENGTH);
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
