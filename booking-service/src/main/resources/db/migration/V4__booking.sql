CREATE TABLE booking (
    id           VARCHAR(100) PRIMARY KEY,
    show_id      UUID NOT NULL,
    seat_ids     TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    state        VARCHAR(20) NOT NULL
                     CHECK (state IN ('PENDING', 'HELD', 'PAID', 'CONFIRMED', 'CANCELLED', 'REJECTED')),
    version      BIGINT NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);