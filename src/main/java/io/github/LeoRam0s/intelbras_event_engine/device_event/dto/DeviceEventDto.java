package io.github.LeoRam0s.intelbras_event_engine.device_event.dto;

import java.time.OffsetDateTime;

public record DeviceEventDto(
        String eventId,
        String deviceId,
        String messageType,
        String payloadJson,
        OffsetDateTime eventAt
) {
}
