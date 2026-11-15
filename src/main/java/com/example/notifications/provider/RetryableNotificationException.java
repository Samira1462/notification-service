package com.example.notifications.provider;

/**
 * A temporary failure (timeout, throttling, 5xx). The consumer retries with exponential backoff and
 * dead-letters the event only after retries are exhausted.
 */
public class RetryableNotificationException extends RuntimeException {

    public RetryableNotificationException(String message) {
        super(message);
    }

    public RetryableNotificationException(String message, Throwable cause) {
        super(message, cause);
    }
}
