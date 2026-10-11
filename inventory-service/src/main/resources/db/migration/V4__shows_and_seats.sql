CREATE TABLE show (
    id         UUID PRIMARY KEY,
    title      VARCHAR(200) NOT NULL,
    venue      VARCHAR(200) NOT NULL,
    starts_at  TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- booking_id is text, not uuid, to match how the outbox/inbox layer already
-- treats aggregate ids as opaque strings throughout the message envelope.
CREATE TABLE seat (
    show_id         UUID NOT NULL REFERENCES show (id),
    seat_label      VARCHAR(10) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'AVAILABLE'
                        CHECK (status IN ('AVAILABLE', 'HELD', 'BOOKED')),
    booking_id      VARCHAR(100),
    hold_expires_at TIMESTAMPTZ,
    PRIMARY KEY (show_id, seat_label)
);

CREATE INDEX idx_seat_held_expiry
    ON seat (hold_expires_at)
    WHERE status = 'HELD';