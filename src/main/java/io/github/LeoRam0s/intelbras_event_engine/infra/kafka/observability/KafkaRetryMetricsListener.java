package io.github.LeoRam0s.intelbras_event_engine.infra.kafka.observability;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaRetryMetricsListener implements RetryListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaRetryMetricsListener.class);

    private final MeterRegistry meterRegistry;

    public KafkaRetryMetricsListener(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void failedDelivery(
            @NonNull ConsumerRecord<?, ?> record,
            Exception exception,
            int deliveryAttempt
    ) {
        this.meterRegistry.counter("kafka.consumer.delivery.failure", "topic", record.topic()).increment();
        log.atWarn()
                .addKeyValue("topic", record.topic())
                .addKeyValue("partition", record.partition())
                .addKeyValue("offset", record.offset())
                .addKeyValue("key", record.key())
                .addKeyValue("attempt", deliveryAttempt)
                .addKeyValue("outcome", "attempt_failed")
                .addKeyValue("errorType", exception.getClass().getSimpleName())
                .log("Kafka event processing failed");
    }

    @Override
    public void recovered(
            @NonNull ConsumerRecord<?, ?> record,
            Exception exception
    ) {
        this.meterRegistry.counter("kafka.consumer.dlt", "topic", record.topic(), "outcome", "success")
                .increment();
        log.atWarn()
                .addKeyValue("topic", record.topic())
                .addKeyValue("partition", record.partition())
                .addKeyValue("offset", record.offset())
                .addKeyValue("key", record.key())
                .addKeyValue("outcome", "sent_to_dlt")
                .addKeyValue("errorType", exception.getClass().getSimpleName())
                .log("Kafka event sent to DLT after retries exhausted");
    }

    @Override
    public void recoveryFailed(
            @NonNull ConsumerRecord<?, ?> record,
            Exception originalException,
            @NonNull Exception recoveryException
    ) {
        this.meterRegistry.counter("kafka.consumer.dlt", "topic", record.topic(), "outcome", "failure")
                .increment();
        log.atError()
                .addKeyValue("topic", record.topic())
                .addKeyValue("partition", record.partition())
                .addKeyValue("offset", record.offset())
                .addKeyValue("key", record.key())
                .addKeyValue("outcome", "dlt_publish_failure")
                .addKeyValue("originalErrorType", originalException.getClass().getSimpleName())
                .addKeyValue("recoveryErrorType", recoveryException.getClass().getSimpleName())
                .setCause(recoveryException)
                .log("Failed to publish Kafka event to DLT");
    }
}
