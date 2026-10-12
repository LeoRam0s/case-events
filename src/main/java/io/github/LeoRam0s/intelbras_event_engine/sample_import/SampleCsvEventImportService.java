package io.github.LeoRam0s.intelbras_event_engine.sample_import;

import io.github.LeoRam0s.intelbras_event_engine.device_event.DeviceEventProducer;
import io.github.LeoRam0s.intelbras_event_engine.device_event.IdempotencyKeyGenerator;
import io.github.LeoRam0s.intelbras_event_engine.device_event.dto.DeviceEventDto;
import io.github.LeoRam0s.intelbras_event_engine.sample_import.exceptions.CsvSampleImportException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;

@Service
public class SampleCsvEventImportService {
    private static final Logger log = LoggerFactory.getLogger(SampleCsvEventImportService.class);

    private final ObjectMapper objectMapper;
    private final DeviceEventProducer deviceEventProducer;

    public SampleCsvEventImportService(
            ObjectMapper objectMapper,
            DeviceEventProducer deviceEventProducer
    ) {
        this.objectMapper = Objects.requireNonNull(objectMapper);
        this.deviceEventProducer = Objects.requireNonNull(deviceEventProducer);
    }

    public void readSample() {
        try (InputStreamReader reader = new InputStreamReader(
                new ClassPathResource("case_backend_eventos_sanitizados.csv").getInputStream(), StandardCharsets.UTF_8);
             CSVParser parser = CSVParser.parse(reader, CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .build())) {

            List<String> sampleCsvHeader = List.of(
                    "event_number", "timestamp_log", "msgType", "did", "dname", "pid",
                    "parentDid", "parentPid", "utcTime", "userId", "oauthProject", "content", "body_completo"
            );

            if (!sampleCsvHeader.equals(parser.getHeaderNames())) {
                throw new CsvSampleImportException("Unexpected CSV header: " + parser.getHeaderNames());
            }

            for (CSVRecord record : parser) {
                DeviceEventDto event;
                try {
                    event = parseRecord(record);
                } catch (RuntimeException exception) {
                    log.warn("Skipping invalid CSV line {}: {}", parser.getCurrentLineNumber(), exception.getMessage());
                    continue;
                }

                deviceEventProducer.publish(event);
            }
        } catch (IOException | UncheckedIOException exception) {
            throw new CsvSampleImportException("Unable to read the sample CSV", exception);
        }
    }

    private DeviceEventDto parseRecord(CSVRecord record) {
        if (!record.isConsistent()) {
            throw new IllegalArgumentException("Wrong number of columns");
        }

        String body = required(record.get("body_completo"), "body_completo");
        JsonNode payload = objectMapper.readTree(body);
        if (!payload.isObject()) {
            throw new IllegalArgumentException("body_completo must be a JSON object");
        }

        String deviceId = payload.path("did").asText("");
        if (deviceId.isBlank()) {
            deviceId = payload.path("deviceId").asText("");
        }

        return new DeviceEventDto(
                IdempotencyKeyGenerator.generate(body),
                required(deviceId, "did or deviceId"),
                required(record.get("msgType"), "msgType"),
                body,
                OffsetDateTime.parse(required(record.get("timestamp_log"), "timestamp_log"))
        );
    }

    private static String required(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing " + field);
        }
        return value;
    }
}
