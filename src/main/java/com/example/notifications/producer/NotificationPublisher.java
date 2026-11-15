package com.example.notifications.producer;

import com.example.notifications.config.NotificationProperties;
import com.example.notifications.model.NotificationEvent;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * What business services (orders, payments, auth) call instead of talking to email/SMS/push providers.
 * Events are keyed by user id, so one user's notifications stay ordered within a partition.
 */
@Component
public class NotificationPublisher {

    private static final Logger log = LoggerFactory.getLogger(NotificationPublisher.class);

    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;
    private final NotificationProperties properties;

    public NotificationPublisher(KafkaTemplate<String, NotificationEvent> kafkaTemplate, NotificationProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public CompletableFuture<SendResult<String, NotificationEvent>> publish(NotificationEvent event) {
        String topic = properties.topicFor(event.type());
        return kafkaTemplate.send(topic, event.userId(), event).whenComplete((result, error) -> {
            if (error != null) {
                log.error("Failed to publish event {} to {}", event.eventId(), topic, error);
            } else {
                log.debug("Published event {} to {}-{}@{}", event.eventId(), topic,
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }
}
