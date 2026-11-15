package com.example.notifications.consumer;

import com.example.notifications.model.NotificationEvent;
import com.example.notifications.service.NotificationService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Single-topic layout: one stream of all notification activity, routed to the right channel by
 * {@link NotificationService}. Simpler for producers, but all channels share partitions and threads.
 */
@Component
@ConditionalOnProperty(name = "notification.topic-mode", havingValue = "SHARED")
public class SharedTopicListener {

    private final NotificationService service;

    public SharedTopicListener(NotificationService service) {
        this.service = service;
    }

    @KafkaListener(
            id = "notification-worker",
            topics = "${notification.topics.shared}",
            concurrency = "${notification.shared-concurrency}")
    public void onEvent(NotificationEvent event) {
        service.process(event);
    }
}
