package com.debankar.saga.inventory.seat;

import java.time.Instant;
import java.util.UUID;

public record Show(
        UUID id,
        String title,
        String venue,
        Instant startsAt
) {
}