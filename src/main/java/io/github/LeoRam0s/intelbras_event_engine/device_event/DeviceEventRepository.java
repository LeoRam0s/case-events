package io.github.LeoRam0s.intelbras_event_engine.device_event;

import io.github.LeoRam0s.intelbras_event_engine.device_event.dto.DeviceEventDto;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class DeviceEventRepository {
    private final JdbcTemplate jdbcTemplate;

    public DeviceEventRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean insertConsumeEventIfAbsent(DeviceEventDto event) {
        int inserted = jdbcTemplate.update("""
            INSERT INTO tb_consumed_events
                (pk_event_id, device_id, message_type, payload, event_at)
            VALUES (?, ?, ?, ?::jsonb, ?)
            ON CONFLICT (pk_event_id) DO NOTHING
            """,
                event.eventId(),
                event.deviceId(),
                event.messageType(),
                event.payloadJson(),
                event.eventAt()
        );

        return inserted == 1;
    }
}
