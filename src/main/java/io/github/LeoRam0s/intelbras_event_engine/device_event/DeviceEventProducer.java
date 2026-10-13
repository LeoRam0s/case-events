package io.github.LeoRam0s.intelbras_event_engine.device_event;

import io.github.LeoRam0s.intelbras_event_engine.device_event.dto.DeviceEventDto;
import io.github.LeoRam0s.intelbras_event_engine.device_event.observability.DeviceEventMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class DeviceEventProducer {
    private static final Logger log = LoggerFactory.getLogger(DeviceEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String eventsTopic;
    private final DeviceEventMetrics deviceEventMetrics;

    public DeviceEventProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.topics.device-events.name:device-events}") String eventsTopic,
            DeviceEventMetrics deviceEventMetrics
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventsTopic = eventsTopic;
        this.deviceEventMetrics = deviceEventMetrics;
    }

    public void publish(DeviceEventDto event) {
        final CompletableFuture<SendResult<String, Object>> sendFuture;

        try {
            sendFuture = kafkaTemplate.send(eventsTopic, event.deviceId(), event);
        }
        catch (RuntimeException | Error failure) {
            deviceEventMetrics.recordPublishFailure();
            throw failure;
        }

        sendFuture
                .whenComplete((result, failure) -> {
                    if (failure != null) {
                        deviceEventMetrics.recordPublishFailure();
                        log.atError()
                                .addKeyValue("topic", eventsTopic)
                                .addKeyValue("eventId", event.eventId())
                                .addKeyValue("deviceId", event.deviceId())
                                .addKeyValue("outcome", "failure")
                                .setCause(failure)
                                .log("Failed to publish event");
                    }
                    else {
                        deviceEventMetrics.recordPublishSuccess();
                    }
                });
    }
}
