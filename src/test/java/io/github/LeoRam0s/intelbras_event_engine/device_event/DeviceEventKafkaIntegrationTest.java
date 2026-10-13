package io.github.LeoRam0s.intelbras_event_engine.device_event;

import io.github.LeoRam0s.intelbras_event_engine.device_event.dto.DeviceEventDto;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockReset;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
class DeviceEventKafkaIntegrationTest {

    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(30);

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    // The custom DLT producer factory reads KafkaProperties directly.
    @DynamicPropertySource
    static void kafkaBootstrapServers(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private DeviceEventProducer deviceEventProducer;

    @MockitoSpyBean(reset = MockReset.AFTER)
    private DeviceEventService deviceEventService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    @Value("${app.kafka.topics.device-events.name}")
    private String topic;

    @BeforeEach
    void clearPersistedEvents() {
        jdbcTemplate.execute("TRUNCATE TABLE tb_consumed_events");
    }

    @Test
    void shouldPublishConsumeProcessAndPersistValidEvent() {
        DeviceEventDto event = event("{\"temperature\":21,\"enabled\":true}");

        deviceEventProducer.publish(event);

        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> assertThat(count(event.eventId())).isEqualTo(1));
        PersistedEvent persisted = load(event.eventId());
        assertThat(persisted.deviceId()).isEqualTo(event.deviceId());
        assertThat(persisted.messageType()).isEqualTo(event.messageType());
        assertThat(persisted.eventAt().toInstant()).isEqualTo(event.eventAt().toInstant());
        assertThat(persisted.processedAt()).isNotNull();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT payload = ?::jsonb FROM tb_consumed_events WHERE pk_event_id = ?",
                Boolean.class,
                "{ \"enabled\": true, \"temperature\": 21 }",
                event.eventId()
        )).isTrue();
    }

    @Test
    void shouldPersistEventOnlyOnceWhenSameEventIsReprocessed() {
        DeviceEventDto event = event("{\"temperature\":21}");
        double duplicateBaseline = duplicateCount();

        deviceEventProducer.publish(event);
        deviceEventProducer.publish(event);

        await().atMost(WAIT_TIMEOUT).untilAsserted(
                () -> assertThat(duplicateCount()).isEqualTo(duplicateBaseline + 1)
        );
        assertThat(count(event.eventId())).isEqualTo(1);
    }

    @Test
    void shouldRetryAndSendToDltWhenProcessingKeepsFailing() {
        DeviceEventDto malformed = event("{invalid-json");
        double retryBaseline = retryCount();

        deviceEventProducer.publish(malformed);

        ConsumerRecord<String, byte[]> deadLetter = awaitDltRecord(malformed.deviceId());
        assertThat(deadLetter.topic()).isEqualTo(topic + "-dlt");
        assertThat(retryCount()).isGreaterThan(retryBaseline);
        assertThat(count(malformed.eventId())).isZero();

        DeviceEventDto following = event("{\"temperature\":22}");
        deviceEventProducer.publish(following);
        await().atMost(WAIT_TIMEOUT).untilAsserted(() -> assertThat(count(following.eventId())).isEqualTo(1));
    }

    @Test
    void shouldSendIllegalArgumentExceptionDirectlyToDltWithoutRetry() {
        DeviceEventDto event = event("{\"temperature\":23}");
        double retryBaseline = retryCount();
        doThrow(new IllegalArgumentException("injected failure"))
                .when(deviceEventService).process(event);

        deviceEventProducer.publish(event);

        assertThat(awaitDltRecord(event.deviceId()).topic()).isEqualTo(topic + "-dlt");
        verify(deviceEventService, times(1)).process(event);
        assertThat(retryCount()).isEqualTo(retryBaseline);
        assertThat(count(event.eventId())).isZero();
    }

    @Test
    void shouldSendDeserializationFailureToDltWithoutPersistingAnything() throws Exception {
        String key = "invalid-device-" + UUID.randomUUID();
        byte[] invalidBytes = "not a JSON document".getBytes(StandardCharsets.UTF_8);
        double consumedBaseline = consumedCount();

        try (KafkaProducer<String, byte[]> rawProducer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
        ))) {
            rawProducer.send(new ProducerRecord<>(topic, key, invalidBytes)).get(10, TimeUnit.SECONDS);
        }

        ConsumerRecord<String, byte[]> deadLetter = awaitDltRecord(key);
        assertArrayEquals(invalidBytes, deadLetter.value());
        assertThat(consumedCount()).isEqualTo(consumedBaseline);
        assertThat(countAll()).isZero();
    }

    private DeviceEventDto event(String payloadJson) {
        return new DeviceEventDto(
                "event-" + UUID.randomUUID(),
                "device-" + UUID.randomUUID(),
                "temperature.changed",
                payloadJson,
                OffsetDateTime.parse("2026-10-13T12:00:00Z")
        );
    }

    private ConsumerRecord<String, byte[]> awaitDltRecord(String expectedKey) {
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-observer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
        ))) {
            consumer.subscribe(List.of(topic + "-dlt"));
            Instant deadline = Instant.now().plus(WAIT_TIMEOUT);
            while (Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(250))) {
                    if (expectedKey.equals(record.key())) {
                        return record;
                    }
                }
            }
            return fail("No DLT record for key " + expectedKey + " within " + WAIT_TIMEOUT);
        }
    }

    private int count(String eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tb_consumed_events WHERE pk_event_id = ?", Integer.class, eventId
        );
    }

    private int countAll() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tb_consumed_events", Integer.class);
    }

    private PersistedEvent load(String eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT device_id, message_type, event_at, processed_at "
                        + "FROM tb_consumed_events WHERE pk_event_id = ?",
                (rs, rowNum) -> new PersistedEvent(
                        rs.getString("device_id"),
                        rs.getString("message_type"),
                        rs.getObject("event_at", OffsetDateTime.class),
                        rs.getObject("processed_at", OffsetDateTime.class)
                ),
                eventId
        );
    }

    private double duplicateCount() {
        return meterRegistry.get("device.events.processed").tag("outcome", "duplicate").counter().count();
    }

    private double retryCount() {
        return meterRegistry.get("device.events.retry").counter().count();
    }

    private double consumedCount() {
        return meterRegistry.get("device.events.consumed").counter().count();
    }

    private record PersistedEvent(
            String deviceId,
            String messageType,
            OffsetDateTime eventAt,
            OffsetDateTime processedAt
    ) {
    }
}
