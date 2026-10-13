package io.github.LeoRam0s.intelbras_event_engine.device_event.kafka;

import io.github.LeoRam0s.intelbras_event_engine.device_event.DeviceEventService;
import io.github.LeoRam0s.intelbras_event_engine.device_event.dto.DeviceEventDto;
import io.github.LeoRam0s.intelbras_event_engine.device_event.observability.DeviceEventMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.ByteBuffer;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeviceEventConsumerTest {

    private SimpleMeterRegistry simpleMeterRegistry;
    private DeviceEventService deviceEventService;
    private DeviceEventConsumer deviceEventConsumer;

    @BeforeEach
    void setUp() {
        simpleMeterRegistry = new SimpleMeterRegistry();
        deviceEventService = mock(DeviceEventService.class);
        deviceEventConsumer = new DeviceEventConsumer(deviceEventService, new DeviceEventMetrics(simpleMeterRegistry));
    }

    @AfterEach
    void closeRegistry() {
        simpleMeterRegistry.close();
    }

    @Test
    void shouldProcessInsertedEventAndRecordMetrics() {
        DeviceEventDto deviceEventDto = validEvent();
        ConsumerRecord<String, DeviceEventDto> record = record(deviceEventDto);
        when(deviceEventService.process(deviceEventDto)).thenReturn(true);

        deviceEventConsumer.consume(record);

        verify(deviceEventService, times(1)).process(deviceEventDto);
        assertCounter("device.events.consumed", 1.0);
        assertCounter("device.events.retry", 0.0);
        assertProcessedCounter("inserted", 1.0);
        assertProcessedCounter("duplicate", 0.0);
        assertProcessingCount("inserted", 1L);
        assertProcessingCount("duplicate", 0L);
        assertProcessingCount("failure", 0L);
    }

    @Test
    void shouldClassifyEventAsDuplicateWhenServiceDoesNotInsertIt() {
        DeviceEventDto deviceEventDto = validEvent();
        ConsumerRecord<String, DeviceEventDto> record = record(deviceEventDto);
        when(deviceEventService.process(deviceEventDto)).thenReturn(false);

        deviceEventConsumer.consume(record);

        verify(deviceEventService, times(1)).process(deviceEventDto);
        assertCounter("device.events.consumed", 1.0);
        assertProcessedCounter("inserted", 0.0);
        assertProcessedCounter("duplicate", 1.0);
        assertProcessingCount("inserted", 0L);
        assertProcessingCount("duplicate", 1L);
        assertProcessingCount("failure", 0L);
    }

    @Test
    void shouldRecordRetryWhenDeliveryAttemptIsGreaterThanOne() {
        DeviceEventDto deviceEventDto = validEvent();
        ConsumerRecord<String, DeviceEventDto> record = recordWithDeliveryAttempt(deviceEventDto, 2);
        when(deviceEventService.process(deviceEventDto)).thenReturn(true);

        deviceEventConsumer.consume(record);

        verify(deviceEventService, times(1)).process(deviceEventDto);
        assertCounter("device.events.consumed", 1.0);
        assertCounter("device.events.retry", 1.0);
        assertProcessedCounter("inserted", 1.0);
        assertProcessingCount("inserted", 1L);
    }

    @Test
    void shouldRecordProcessingFailureAndRethrowException() {
        DeviceEventDto deviceEventDto = validEvent();
        ConsumerRecord<String, DeviceEventDto> record = record(deviceEventDto);
        IllegalStateException failure = new IllegalStateException("processing failed");
        when(deviceEventService.process(deviceEventDto)).thenThrow(failure);

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> deviceEventConsumer.consume(record));

        assertSame(failure, thrown);
        verify(deviceEventService, times(1)).process(deviceEventDto);
        assertCounter("device.events.consumed", 1.0);
        assertCounter("device.events.retry", 0.0);
        assertProcessedCounter("inserted", 0.0);
        assertProcessedCounter("duplicate", 0.0);
        assertProcessingCount("failure", 1L);
        assertProcessingCount("inserted", 0L);
        assertProcessingCount("duplicate", 0L);
    }

    private DeviceEventDto validEvent() {
        return new DeviceEventDto(
                "event-1",
                "device-1",
                "temperature",
                "{\"value\":21}",
                OffsetDateTime.parse("2026-10-13T12:00:00Z")
        );
    }

    private ConsumerRecord<String, DeviceEventDto> record(DeviceEventDto event) {
        return new ConsumerRecord<>("device-events", 0, 10L, "event-1", event);
    }

    private ConsumerRecord<String, DeviceEventDto> recordWithDeliveryAttempt(
            DeviceEventDto event,
            int deliveryAttempt
    ) {
        ConsumerRecord<String, DeviceEventDto> record = record(event);
        record.headers().add(
                KafkaHeaders.DELIVERY_ATTEMPT,
                ByteBuffer.allocate(Integer.BYTES).putInt(deliveryAttempt).array()
        );
        return record;
    }

    private void assertCounter(String name, double expected) {
        assertEquals(expected, simpleMeterRegistry.get(name).counter().count());
    }

    private void assertProcessedCounter(String outcome, double expected) {
        assertEquals(expected, simpleMeterRegistry.get("device.events.processed").tag("outcome", outcome).counter().count());
    }

    private void assertProcessingCount(String outcome, long expected) {
        assertEquals(expected, simpleMeterRegistry.get("device.events.processing").tag("outcome", outcome).timer().count());
    }
}
