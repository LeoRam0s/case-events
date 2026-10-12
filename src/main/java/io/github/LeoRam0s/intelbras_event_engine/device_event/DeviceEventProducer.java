package io.github.LeoRam0s.intelbras_event_engine.device_event;

import io.github.LeoRam0s.intelbras_event_engine.device_event.dto.DeviceEventDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class DeviceEventProducer {
    private static final Logger log = LoggerFactory.getLogger(DeviceEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String eventsTopic;

    public DeviceEventProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.topics.device-events.name:device-events}") String eventsTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventsTopic = eventsTopic;
    }

    public void publish(DeviceEventDto event) {
        kafkaTemplate.send(eventsTopic, event.deviceId(), event)
                .whenComplete((result, failure) -> {
                    if (failure != null) {
                        log.error(
                                "Failed to publish event eventId={}, deviceId={}",
                                event.eventId(),
                                event.deviceId(),
                                failure
                        );
                    }
                });
    }
}
