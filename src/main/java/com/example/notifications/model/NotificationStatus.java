package com.example.notifications.model;

public enum NotificationStatus {
    /** Accepted (or being retried) and waiting for a worker. */
    PENDING,
    /** A worker is currently attempting delivery. */
    PROCESSING,
    /** The provider accepted the notification. */
    SENT,
    /** Gave up: non-retryable error or retries exhausted. The event is on the dead-letter topic. */
    FAILED
}
