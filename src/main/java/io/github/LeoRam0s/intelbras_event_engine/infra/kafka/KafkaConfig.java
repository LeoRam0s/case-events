package io.github.LeoRam0s.intelbras_event_engine.infra.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.LinkedHashMap;

@Configuration(proxyBeanMethods = false)
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    public ProducerFactory<String, Object> dltProducerFactory(KafkaProperties kafkaProperties) {
        var valueSerializers = new LinkedHashMap<Class<?>, Serializer<?>>();

        valueSerializers.put(Object.class, new JacksonJsonSerializer<>());

        valueSerializers.put(byte[].class, new ByteArraySerializer());

        return new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(),
                new StringSerializer(),
                new DelegatingByTypeSerializer(valueSerializers, true)
        );
    }

    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate(
            ProducerFactory<String, Object> dltProducerFactory
    ) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    @Bean
    public CommonErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, Object> dltKafkaTemplate,
            @Value("${app.kafka.retry.max-retries:5}") int maxRetries,
            @Value("${app.kafka.retry.initial-interval-ms:2000}") long initialIntervalMs,
            @Value("${app.kafka.retry.multiplier:2.0}") double multiplier,
            @Value("${app.kafka.retry.max-interval-ms:30000}") long maxIntervalMs
    ) {
        var recoverer = new DeadLetterPublishingRecoverer(dltKafkaTemplate);
        recoverer.setFailIfSendResultIsError(true);

        var backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxIntervalMs);

        var errorHandler = new DefaultErrorHandler(recoverer, backOff);

        errorHandler.setRetryListeners(new RetryListener() {

            @Override
            public void failedDelivery(
                    @NonNull ConsumerRecord<?, ?> record,
                    Exception exception,
                    int deliveryAttempt
            ) {
                log.warn(
                        "Kafka event processing failed: topic={}, partition={}, offset={}, key={}, attempt={}, error={}",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        deliveryAttempt,
                        exception.getMessage()
                );
            }

            @Override
            public void recovered(
                    @NonNull ConsumerRecord<?, ?> record,
                    Exception exception
            ) {
                log.warn(
                        "Kafka event sent to DLT after retries exhausted: topic={}, partition={}, offset={}, key={}, error={}",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        exception.getMessage()
                );
            }

            @Override
            public void recoveryFailed(
                    @NonNull ConsumerRecord<?, ?> record,
                    Exception originalException,
                    @NonNull Exception recoveryException
            ) {
                log.error(
                        "Failed to publish Kafka event to DLT: topic={}, partition={}, offset={}, key={}, originalError={}",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        originalException.getMessage(),
                        recoveryException
                );
            }
        });

        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);

        return errorHandler;
    }
}
