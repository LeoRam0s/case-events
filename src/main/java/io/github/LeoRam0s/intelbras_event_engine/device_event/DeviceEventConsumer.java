package io.github.LeoRam0s.intelbras_event_engine.device_event;

import io.github.LeoRam0s.intelbras_event_engine.device_event.dto.DeviceEventDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DeviceEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(DeviceEventConsumer.class);
    private final DeviceEventService deviceEventService;

    public DeviceEventConsumer(DeviceEventService deviceEventService) {
        this.deviceEventService = deviceEventService;
    }

    @KafkaListener(
            id = "deviceEventConsumer",
            topics = "${app.kafka.topics.device-events.name:device-events}",
            groupId = "${spring.application.name}"
    )
    public void consume(ConsumerRecord<String, DeviceEventDto> record) {
        boolean inserted = deviceEventService.process(record.value());

        log.info(
                "Kafka event consumed: topic={}, partition={}, offset={}, key={}, eventId={}, inserted={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value().eventId(),
                inserted
        );
    }
}
