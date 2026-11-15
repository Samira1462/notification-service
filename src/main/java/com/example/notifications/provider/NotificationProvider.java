package com.example.notifications.provider;

import com.example.notifications.model.NotificationType;

/** Adapter around one external delivery provider (SES, Twilio, FCM, ...). */
public interface NotificationProvider {

    NotificationType channel();

    /**
     * Delivers one message.
     *
     * @param idempotencyKey stable per event; pass it to providers that support idempotency keys so a
     *     resend after a crash is not delivered twice
     * @throws RetryableNotificationException for temporary failures
     * @throws NonRetryableNotificationException for permanent failures
     */
    void send(String idempotencyKey, String recipient, String subject, String body);
}
