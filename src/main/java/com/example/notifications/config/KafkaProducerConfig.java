package com.example.notifications.config;

import com.example.notifications.model.NotificationEvent;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Qualifier;
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
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, NotificationEvent> eventProducerFactory(
            KafkaProperties kafkaProperties, MeterRegistry meterRegistry) {
        DefaultKafkaProducerFactory<String, NotificationEvent> factory =
                new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(null));
        factory.addListener(new MicrometerProducerListener<>(meterRegistry));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, NotificationEvent> kafkaTemplate(
            @Qualifier("eventProducerFactory") ProducerFactory<String, NotificationEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ProducerFactory<String, byte[]> dlqBytesProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> config = kafkaProperties.buildProducerProperties(null);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, byte[]> dlqBytesKafkaTemplate(
            @Qualifier("dlqBytesProducerFactory") ProducerFactory<String, byte[]> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
