package io.github.LeoRam0s.intelbras.deviceevent.dto;

import java.time.OffsetDateTime;

public record DeviceEventDto(
        String eventId,
        String deviceId,
        String messageType,
        String payloadJson,
        OffsetDateTime eventAt
) {
}
