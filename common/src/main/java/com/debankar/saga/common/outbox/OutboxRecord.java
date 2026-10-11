package com.debankar.saga.common.outbox;

import com.debankar.saga.common.messaging.MessageEnvelope;

import java.time.Instant;
import java.util.UUID;

/**
 * One row of the {@code outbox_message} table, written in the same
 * database transaction as the business change it announces.
 */
public record OutboxRecord(
        UUID id,
        String aggregateType,
        String aggregateId,
        String topic,
        String messageKey,
        String eventType,
        String payload,
        Instant createdAt
) {

    public static OutboxRecord newRecord(String aggregateType, String aggregateId,
                                         String topic, String messageKey,
                                         String eventType, String payloadJson) {
        return new OutboxRecord(UUID.randomUUID(), aggregateType, aggregateId,
                topic, messageKey, eventType, payloadJson, Instant.now());
    }

    public MessageEnvelope toEnvelope() {
        return new MessageEnvelope(id, eventType, aggregateType, aggregateId, createdAt, payload);
    }
}