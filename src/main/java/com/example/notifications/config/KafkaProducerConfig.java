package com.example.notifications.config;

import com.example.notifications.model.NotificationEvent;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Two producers:
 *
 * <ul>
 *   <li>{@code kafkaTemplate} publishes {@link NotificationEvent}s as JSON (used by the API, replay and
 *       the dead-letter recoverer for events that deserialised fine).
 *   <li>{@code dlqBytesKafkaTemplate} republishes the raw bytes of records that could not be
 *       deserialised, so a poison pill lands on the DLQ unchanged.
 * </ul>
 *
 * <p>Both take the broker address from {@link KafkaConnectionDetails}, like Spring Boot's own
 * auto-configured clients. That is normally {@code spring.kafka.bootstrap-servers}, but it can also come
 * from a service connection (e.g. a Testcontainers Kafka container); reading only the properties would
 * make these producers write to a different broker than the consumers read from.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, NotificationEvent> eventProducerFactory(
            KafkaProperties kafkaProperties, KafkaConnectionDetails connectionDetails, MeterRegistry meterRegistry) {
        DefaultKafkaProducerFactory<String, NotificationEvent> factory =
                new DefaultKafkaProducerFactory<>(producerProperties(kafkaProperties, connectionDetails));
        factory.addListener(new MicrometerProducerListener<>(meterRegistry));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, NotificationEvent> kafkaTemplate(
            @Qualifier("eventProducerFactory") ProducerFactory<String, NotificationEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ProducerFactory<String, byte[]> dlqBytesProducerFactory(
            KafkaProperties kafkaProperties, KafkaConnectionDetails connectionDetails) {
        Map<String, Object> config = producerProperties(kafkaProperties, connectionDetails);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, byte[]> dlqBytesKafkaTemplate(
            @Qualifier("dlqBytesProducerFactory") ProducerFactory<String, byte[]> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    private static Map<String, Object> producerProperties(
            KafkaProperties kafkaProperties, KafkaConnectionDetails connectionDetails) {
        Map<String, Object> config = kafkaProperties.buildProducerProperties(null);
        KafkaConnectionDetails.Configuration producer = connectionDetails.getProducer();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producer.getBootstrapServers());
        if (producer.getSecurityProtocol() != null) {
            config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, producer.getSecurityProtocol());
        }
        return config;
    }
}
