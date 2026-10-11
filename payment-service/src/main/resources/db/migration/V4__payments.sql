CREATE TABLE payment (
    booking_id   VARCHAR(100) PRIMARY KEY,
    status       VARCHAR(20) NOT NULL CHECK (status IN ('AUTHORIZED', 'DECLINED', 'VOIDED')),
    amount_cents BIGINT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);