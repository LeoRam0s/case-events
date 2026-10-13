package io.github.LeoRam0s.intelbras_event_engine.device_event;

import io.github.LeoRam0s.intelbras_event_engine.device_event.dto.DeviceEventDto;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeviceEventServiceTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldReturnRepositoryProcessingResult(boolean inserted) {
        DeviceEventRepository deviceEventRepository = mock();
        DeviceEventService deviceEventService = new DeviceEventService(deviceEventRepository);
        DeviceEventDto deviceEventDto = new DeviceEventDto(
                "event-1",
                "device-1",
                "STATUS",
                "{}",
                OffsetDateTime.parse("2026-10-13T12:00:00Z")
        );
        when(deviceEventRepository.insertConsumeEventIfAbsent(deviceEventDto)).thenReturn(inserted);

        boolean result = deviceEventService.process(deviceEventDto);

        assertThat(result).isEqualTo(inserted);
        verify(deviceEventRepository).insertConsumeEventIfAbsent(deviceEventDto);
    }
}
