package io.github.LeoRam0s.intelbras_event_engine.sample_import;

import io.github.LeoRam0s.intelbras_event_engine.device_event.DeviceEventProducer;
import io.github.LeoRam0s.intelbras_event_engine.device_event.IdempotencyKeyGenerator;
import io.github.LeoRam0s.intelbras_event_engine.device_event.dto.DeviceEventDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.ObjectMapper;

import java.time.OffsetDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SampleCsvEventImportServiceTest {

    // Expected valid records in the official classpath CSV.
    private static final int EXPECTED_VALID_EVENT_COUNT = 101;

    private DeviceEventProducer deviceEventProducer;
    private SampleCsvEventImportService sampleCsvEventImportService;
    private ArgumentCaptor<DeviceEventDto> deviceEventCaptor;

    @BeforeEach
    void setUp() {
        deviceEventProducer = mock(DeviceEventProducer.class);
        sampleCsvEventImportService = new SampleCsvEventImportService(new ObjectMapper(), deviceEventProducer);
        deviceEventCaptor = ArgumentCaptor.forClass(DeviceEventDto.class);
    }

    @Test
    void shouldReadSampleCsvAndPublishValidEvents() {
        assertThatCode(sampleCsvEventImportService::readSample).doesNotThrowAnyException();

        verify(deviceEventProducer, times(EXPECTED_VALID_EVENT_COUNT)).publish(deviceEventCaptor.capture());
        assertThat(deviceEventCaptor.getAllValues()).hasSize(EXPECTED_VALID_EVENT_COUNT);
    }

    @Test
    void shouldMapCsvRecordToDeviceEvent() {
        sampleCsvEventImportService.readSample();

        verify(deviceEventProducer, times(EXPECTED_VALID_EVENT_COUNT)).publish(deviceEventCaptor.capture());
        DeviceEventDto firstEvent = deviceEventCaptor.getAllValues().get(0);
        String body = "{\"content\":{\"properties\":{\"11800\":3}},\"did\":\"DEVICE-00014\","
                + "\"dname\":\"Device 00014\",\"msgType\":\"iotProperty\",\"pid\":\"PRODUCT-00004\","
                + "\"utcTime\":\"20260925T084750\",\"appId\":\"APP-CASE\",\"userId\":\"USER-00004\","
                + "\"oauthProject\":\"PROJECT-CASE\"}";

        assertThat(firstEvent.deviceId()).isEqualTo("DEVICE-00014");
        assertThat(firstEvent.messageType()).isEqualTo("iotProperty");
        assertThat(firstEvent.payloadJson()).isEqualTo(body);
        assertThat(firstEvent.eventAt()).isEqualTo(OffsetDateTime.parse("2026-09-25T08:47:51.157000+00:00"));
        assertThat(firstEvent.eventId()).isEqualTo(IdempotencyKeyGenerator.generate(body));
    }

    @Test
    void shouldUseDeviceIdFallbackWhenDidIsMissing() {
        sampleCsvEventImportService.readSample();

        verify(deviceEventProducer, times(EXPECTED_VALID_EVENT_COUNT)).publish(deviceEventCaptor.capture());
        List<DeviceEventDto> events = deviceEventCaptor.getAllValues();

        assertThat(events)
                .filteredOn(event -> event.payloadJson().contains("\"deviceId\":\"FJ1K2001704PS\""))
                .singleElement()
                .extracting(DeviceEventDto::deviceId)
                .isEqualTo("FJ1K2001704PS");
        assertThat(events)
                .filteredOn(event -> event.payloadJson().contains("\"deviceId\":\"IA8L4001890ZN\""))
                .singleElement()
                .extracting(DeviceEventDto::deviceId)
                .isEqualTo("IA8L4001890ZN");
    }

}
