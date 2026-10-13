package io.github.LeoRam0s.intelbras.deviceevent.kafka;

import io.github.LeoRam0s.intelbras.deviceevent.DeviceEventService;
import io.github.LeoRam0s.intelbras.deviceevent.dto.DeviceEventDto;
import io.github.LeoRam0s.intelbras.deviceevent.observability.DeviceEventMetrics;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;

@Component
public class DeviceEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(DeviceEventConsumer.class);
    private final DeviceEventService deviceEventService;
    private final DeviceEventMetrics deviceEventMetrics;

    public DeviceEventConsumer(DeviceEventService deviceEventService, DeviceEventMetrics deviceEventMetrics) {
        this.deviceEventService = deviceEventService;
        this.deviceEventMetrics = deviceEventMetrics;
    }

    @KafkaListener(
            id = "deviceEventConsumer",
            topics = "${app.kafka.topics.device-events.name:device-events}",
            groupId = "${spring.application.name}"
    )
    public void consume(ConsumerRecord<String, DeviceEventDto> record) {
        deviceEventMetrics.recordConsumed();

        int attempt = deliveryAttempt(record);
        if (attempt > 1) {
            deviceEventMetrics.recordRetry();
        }

        Timer.Sample processing = deviceEventMetrics.startProcessing();
        final boolean inserted;

        try {
            inserted = deviceEventService.process(record.value());
        }
        catch (RuntimeException | Error failure) {
            deviceEventMetrics.stopProcessingFailure(processing);
            throw failure;
        }
        deviceEventMetrics.recordProcessed(inserted);
        deviceEventMetrics.stopProcessing(processing, inserted);

        DeviceEventDto event = record.value();
        log.atInfo()
                .addKeyValue("topic", record.topic())
                .addKeyValue("partition", record.partition())
                .addKeyValue("offset", record.offset())
                .addKeyValue("key", record.key())
                .addKeyValue("eventId", event.eventId())
                .addKeyValue("deviceId", event.deviceId())
                .addKeyValue("messageType", event.messageType())
                .addKeyValue("attempt", attempt)
                .addKeyValue("outcome", inserted ? "inserted" : "duplicate")
                .log("Kafka event consumed");
    }

    private int deliveryAttempt(ConsumerRecord<String, DeviceEventDto> record) {
        Header header = record.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
        if (header == null || header.value() == null || header.value().length != Integer.BYTES) {
            return 1;
        }
        return ByteBuffer.wrap(header.value()).getInt();
    }
}
