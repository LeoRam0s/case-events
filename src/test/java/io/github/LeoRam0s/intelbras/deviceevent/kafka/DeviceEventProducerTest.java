package io.github.LeoRam0s.intelbras.deviceevent.kafka;

import io.github.LeoRam0s.intelbras.deviceevent.dto.DeviceEventDto;
import io.github.LeoRam0s.intelbras.deviceevent.observability.DeviceEventMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeviceEventProducerTest {

    private static final String TOPIC = "device-events";

    private SimpleMeterRegistry simpleMeterRegistry;
    private KafkaTemplate<String, Object> kafkaTemplate;
    private DeviceEventProducer deviceEventProducer;

    @BeforeEach
    void setUp() {
        simpleMeterRegistry = new SimpleMeterRegistry();
        kafkaTemplate = mock();
        DeviceEventMetrics deviceEventMetrics = new DeviceEventMetrics(simpleMeterRegistry);
        deviceEventProducer = new DeviceEventProducer(kafkaTemplate, TOPIC, deviceEventMetrics);
    }

    @AfterEach
    void closeRegistry() {
        simpleMeterRegistry.close();
    }

    @Test
    void shouldPublishEventUsingDeviceIdAsKeyAndRecordSuccess() {
        DeviceEventDto deviceEventDto = createDeviceEvent();
        CompletableFuture<SendResult<String, Object>> sendFuture = new CompletableFuture<>();
        when(kafkaTemplate.send(eq(TOPIC), eq(deviceEventDto.deviceId()), same(deviceEventDto))).thenReturn(sendFuture);

        deviceEventProducer.publish(deviceEventDto);

        assertEquals(0.0, publishCount("success"));
        assertEquals(0.0, publishCount("failure"));

        assertFalse(sendFuture.isDone());
        sendFuture.complete(null);

        verify(kafkaTemplate, times(1)).send(eq(TOPIC), eq(deviceEventDto.deviceId()), same(deviceEventDto));
        assertEquals(1.0, publishCount("success"));
        assertEquals(0.0, publishCount("failure"));
    }

    @Test
    void shouldRecordFailureWhenPublishFailsAsynchronously() {
        DeviceEventDto deviceEventDto = createDeviceEvent();
        CompletableFuture<SendResult<String, Object>> sendFuture = new CompletableFuture<>();
        when(kafkaTemplate.send(eq(TOPIC), eq(deviceEventDto.deviceId()), same(deviceEventDto))).thenReturn(sendFuture);

        deviceEventProducer.publish(deviceEventDto);
        sendFuture.completeExceptionally(new IllegalStateException("publish failed"));

        verify(kafkaTemplate, times(1)).send(eq(TOPIC), eq(deviceEventDto.deviceId()), same(deviceEventDto));
        assertEquals(0.0, publishCount("success"));
        assertEquals(1.0, publishCount("failure"));
    }

    @Test
    void shouldRecordFailureAndRethrowWhenPublishFailsSynchronously() {
        DeviceEventDto deviceEventDto = createDeviceEvent();
        IllegalStateException failure = new IllegalStateException("send failed");
        doThrow(failure).when(kafkaTemplate)
                .send(eq(TOPIC), eq(deviceEventDto.deviceId()), same(deviceEventDto));

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> deviceEventProducer.publish(deviceEventDto));

        assertSame(failure, thrown);
        verify(kafkaTemplate, times(1)).send(eq(TOPIC), eq(deviceEventDto.deviceId()), same(deviceEventDto));
        assertEquals(1.0, publishCount("failure"));
        assertEquals(0.0, publishCount("success"));
    }

    private DeviceEventDto createDeviceEvent() {
        return new DeviceEventDto(
                "event-123",
                "device-456",
                "temperature.changed",
                "{\"temperature\":21}",
                OffsetDateTime.parse("2026-10-13T12:00:00-03:00")
        );
    }

    private double publishCount(String outcome) {
        return simpleMeterRegistry.get("device.events.publish")
                .tag("outcome", outcome)
                .counter()
                .count();
    }
}
