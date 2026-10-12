package io.github.LeoRam0s.intelbras_event_engine.device_event;

import io.github.LeoRam0s.intelbras_event_engine.device_event.dto.DeviceEventDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DeviceEventService {
    private static final Logger log = LoggerFactory.getLogger(DeviceEventService.class);

    private final DeviceEventRepository deviceEventRepository;

    public DeviceEventService(DeviceEventRepository deviceEventRepository) {
        this.deviceEventRepository = deviceEventRepository;
    }

    @Transactional
    public boolean process(DeviceEventDto event) {
        log.debug("Processing event eventId={}", event.eventId());

        return deviceEventRepository.insertConsumeEventIfAbsent(event);
    }
}
