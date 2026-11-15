package com.example.notifications.provider;

/**
 * A permanent failure (invalid phone number, unknown template, malformed event). Retrying cannot help,
 * so the consumer sends the event straight to the dead-letter topic.
 */
public class NonRetryableNotificationException extends RuntimeException {

    public NonRetryableNotificationException(String message) {
        super(message);
    }

    public NonRetryableNotificationException(String message, Throwable cause) {
        super(message, cause);
    }
}
