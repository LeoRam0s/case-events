package io.github.LeoRam0s.intelbras_event_engine.device_event.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

@Component
public class DeviceEventMetrics {

    private final MeterRegistry meterRegistry;
    private final Counter publishSuccess;
    private final Counter publishFailure;
    private final Counter consumed;
    private final Counter processedInserted;
    private final Counter processedDuplicate;
    private final Counter retry;
    private final Timer processingInserted;
    private final Timer processingDuplicate;
    private final Timer processingFailure;

    public DeviceEventMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.publishSuccess = meterRegistry.counter("device.events.publish", "outcome", "success");
        this.publishFailure = meterRegistry.counter("device.events.publish", "outcome", "failure");
        this.consumed = meterRegistry.counter("device.events.consumed");
        this.processedInserted = meterRegistry.counter("device.events.processed", "outcome", "inserted");
        this.processedDuplicate = meterRegistry.counter("device.events.processed", "outcome", "duplicate");
        this.retry = meterRegistry.counter("device.events.retry");
        this.processingInserted = meterRegistry.timer("device.events.processing", "outcome", "inserted");
        this.processingDuplicate = meterRegistry.timer("device.events.processing", "outcome", "duplicate");
        this.processingFailure = meterRegistry.timer("device.events.processing", "outcome", "failure");
    }

    public void recordPublishSuccess() {
        this.publishSuccess.increment();
    }

    public void recordPublishFailure() {
        this.publishFailure.increment();
    }

    public void recordConsumed() {
        this.consumed.increment();
    }

    public void recordProcessed(boolean inserted) {
        (inserted ? this.processedInserted : this.processedDuplicate).increment();
    }

    public void recordRetry() {
        this.retry.increment();
    }

    public Timer.Sample startProcessing() {
        return Timer.start(this.meterRegistry);
    }

    public void stopProcessing(Timer.Sample sample, boolean inserted) {
        sample.stop(inserted ? this.processingInserted : this.processingDuplicate);
    }

    public void stopProcessingFailure(Timer.Sample sample) {
        sample.stop(this.processingFailure);
    }
}
