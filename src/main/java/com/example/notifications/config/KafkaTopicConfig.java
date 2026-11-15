package com.example.notifications.config;

import com.example.notifications.config.NotificationProperties.TopicMode;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Declares the topics; Spring Boot's {@link KafkaAdmin} creates any that do not exist at startup. */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public KafkaAdmin.NewTopics notificationTopics(NotificationProperties properties) {
        NotificationProperties.Topics t = properties.topics();
        List<NewTopic> topics = new ArrayList<>();

        if (properties.topicMode() == TopicMode.SHARED) {
            topics.add(topic(t.shared(), t.partitions(), t.replicas()));
        } else {
            topics.add(topic(t.email(), t.partitions(), t.replicas()));
            topics.add(topic(t.sms(), t.partitions(), t.replicas()));
            topics.add(topic(t.push(), t.partitions(), t.replicas()));
        }
        // Dead-lettered events are rare and replayed by hand, so one partition is plenty.
        topics.add(topic(t.dlq(), 1, t.replicas()));

        return new KafkaAdmin.NewTopics(topics.toArray(new NewTopic[0]));
    }

    private static NewTopic topic(String name, int partitions, int replicas) {
        return TopicBuilder.name(name).partitions(partitions).replicas(replicas).build();
    }
}
