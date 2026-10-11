package com.debankar.saga.inventory.seat;

import java.time.Instant;
import java.util.UUID;

public record Seat(
        UUID showId,
        String seatLabel,
        SeatStatus status,
        String bookingId,
        Instant holdExpiresAt
) {
}