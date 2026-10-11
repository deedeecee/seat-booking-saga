CREATE TABLE outbox_message (
    id             UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id   VARCHAR(100) NOT NULL,
    topic          VARCHAR(200) NOT NULL,
    message_key    VARCHAR(200) NOT NULL,
    event_type     VARCHAR(100) NOT NULL,
    payload        TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    published_at   TIMESTAMPTZ
);

CREATE INDEX idx_outbox_message_unpublished
    ON outbox_message (created_at)
    WHERE published_at IS NULL;