package com.debankar.saga.booking.saga;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record Booking(
        String id,
        UUID showId,
        List<String> seatIds,
        long amountCents,
        BookingState state,
        long version,
        Instant createdAt,
        Instant updatedAt
) {
}