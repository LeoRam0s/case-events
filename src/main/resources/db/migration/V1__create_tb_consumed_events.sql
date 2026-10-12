CREATE TABLE tb_consumed_events (
    pk_event_id VARCHAR(64) PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    message_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    event_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_tb_consumed_events_message_type
    ON tb_consumed_events (message_type);

CREATE INDEX idx_tb_consumed_events_device_event_at
    ON tb_consumed_events (device_id, event_at);
