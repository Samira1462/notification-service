package com.example.notifications.consumer;

import com.example.notifications.model.NotificationEvent;
import com.example.notifications.service.NotificationService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * One listener per channel topic. Each channel has its own consumer threads (see
 * {@code notification.concurrency.*}), so a slow SMS provider cannot hold up email, and each channel can
 * be scaled on its own. All listeners share the consumer group {@code notification-workers}.
 */
@Component
@ConditionalOnProperty(name = "notification.topic-mode", havingValue = "PER_CHANNEL", matchIfMissing = true)
public class ChannelTopicListeners {

    private final NotificationService service;

    public ChannelTopicListeners(NotificationService service) {
        this.service = service;
    }

    @KafkaListener(
            id = "email-worker",
            topics = "${notification.topics.email}",
            concurrency = "${notification.concurrency.email}")
    public void onEmail(NotificationEvent event) {
        service.process(event);
    }

    @KafkaListener(
            id = "sms-worker",
            topics = "${notification.topics.sms}",
            concurrency = "${notification.concurrency.sms}")
    public void onSms(NotificationEvent event) {
        service.process(event);
    }

    @KafkaListener(
            id = "push-worker",
            topics = "${notification.topics.push}",
            concurrency = "${notification.concurrency.push}")
    public void onPush(NotificationEvent event) {
        service.process(event);
    }
}
