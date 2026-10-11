package com.debankar.saga.common.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * The wire format published to every topic. {@code messageId} is the same
 * UUID as the originating outbox row, and is what a consumer's inbox table
 * uses to tell a redelivered message from a new one.
 */
public record MessageEnvelope(
        UUID messageId,
        String eventType,
        String aggregateType,
        String aggregateId,
        Instant occurredAt,
        String payload
) {
}