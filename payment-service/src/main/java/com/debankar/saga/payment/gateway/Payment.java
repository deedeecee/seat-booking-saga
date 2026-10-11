package com.debankar.saga.payment.gateway;

import java.time.Instant;

public record Payment(
        String bookingId,
        PaymentStatus status,
        long amountCents,
        Instant createdAt,
        Instant updatedAt
) {
}