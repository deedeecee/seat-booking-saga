package com.debankar.saga.inventory.kafka;

import com.debankar.saga.common.Topics;
import com.debankar.saga.common.inbox.IdempotentConsumer;
import com.debankar.saga.common.messaging.MessageEnvelope;
import com.debankar.saga.common.outbox.OutboxDao;
import com.debankar.saga.common.outbox.OutboxRecord;
import com.debankar.saga.inventory.seat.ConfirmResult;
import com.debankar.saga.inventory.seat.HoldResult;
import com.debankar.saga.inventory.seat.SeatHoldService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;
import java.util.UUID;

/**
 * Consumes every command on {@code inventory.commands} and dispatches by
 * {@code eventType}: {@code HoldSeats}, {@code ConfirmSeats}, and
 * {@code ReleaseSeats} are multiplexed onto one topic, matching how
 * booking-service sends them.
 *
 * <p>{@code concurrency = "3"} matches the topic's own partition count,
 * so up to three commands for three different bookings can be processed
 * in true parallel rather than queueing behind one thread. This is safe
 * specifically because every command and event in this project is keyed
 * by booking id — Kafka's partitioner sends every message for the same
 * booking to the same partition regardless of concurrency, so each
 * booking's own sequence stays strictly ordered even though different
 * bookings now run concurrently. In any environment where the topic has
 * fewer than three partitions — the test suite's own Testcontainers
 * broker, for instance — Spring Kafka documents reducing this down
 * automatically, one partition per container, rather than failing.
 *
 * <p>Each handler runs entirely inside {@link IdempotentConsumer}'s
 * transaction, not one of its own — see this class's own note in the part
 * E write-up for why {@code SeatHoldService}'s independently-built
 * transaction correctly joins rather than nesting. Only JSON parsing and
 * dispatch happen outside any transaction; if the envelope or payload is
 * malformed, the exception propagates straight out of the listener
 * method, where {@code KafkaListenerAutoConfiguration}'s retry-then-dead-
 * letter handling takes over — exactly the poison-message case it exists
 * for.
 */
public class InventoryCommandListener {

    private final SeatHoldService seatHoldService;
    private final IdempotentConsumer idempotentConsumer;
    private final OutboxDao outboxDao;
    private final ObjectMapper objectMapper;

    public InventoryCommandListener(SeatHoldService seatHoldService,
                                    IdempotentConsumer idempotentConsumer,
                                    OutboxDao outboxDao,
                                    ObjectMapper objectMapper) {
        this.seatHoldService = seatHoldService;
        this.idempotentConsumer = idempotentConsumer;
        this.outboxDao = outboxDao;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = "inventory-service", concurrency = "3")
    public void onCommand(String rawEnvelope) throws Exception {
        MessageEnvelope envelope = objectMapper.readValue(rawEnvelope, MessageEnvelope.class);

        switch (envelope.eventType()) {
            case "HoldSeats" -> idempotentConsumer.processOnce(envelope, () -> handleHold(envelope));
            case "ConfirmSeats" -> idempotentConsumer.processOnce(envelope, () -> handleConfirm(envelope));
            case "ReleaseSeats" -> idempotentConsumer.processOnce(envelope, () -> handleRelease(envelope));
            default -> throw new IllegalArgumentException(
                    "Unrecognized inventory command event type: " + envelope.eventType());
        }
    }

    private void handleHold(MessageEnvelope envelope) {
        SeatCommandPayload payload = readPayload(envelope);
        HoldResult result = seatHoldService.holdSeats(payload.showId(), envelope.aggregateId(), payload.seatIds());

        if (result instanceof HoldResult.Held(List<String> seatLabels)) {
            publishEvent(envelope.aggregateId(), "SeatsHeld", new SeatIdsPayload(seatLabels));
        } else if (result instanceof HoldResult.Rejected(String firstUnavailableSeatLabel)) {
            publishEvent(envelope.aggregateId(), "SeatsHoldRejected",
                    new SeatIdPayload(firstUnavailableSeatLabel));
        }
    }

    private void handleConfirm(MessageEnvelope envelope) {
        SeatCommandPayload payload = readPayload(envelope);
        ConfirmResult result =
                seatHoldService.confirmSeats(payload.showId(), envelope.aggregateId(), payload.seatIds());

        if (result instanceof ConfirmResult.Confirmed(List<String> seatLabels)) {
            publishEvent(envelope.aggregateId(), "SeatsConfirmed", new SeatIdsPayload(seatLabels));
        } else if (result instanceof ConfirmResult.Rejected(List<String> requestedSeatLabels)) {
            publishEvent(envelope.aggregateId(), "SeatsConfirmRejected",
                    new SeatIdsPayload(requestedSeatLabels));
        }
    }

    private void handleRelease(MessageEnvelope envelope) {
        // Fire-and-forget compensation: nothing in the saga waits for a
        // reply to ReleaseSeats, so no event is published back.
        SeatCommandPayload payload = readPayload(envelope);
        seatHoldService.releaseSeats(payload.showId(), envelope.aggregateId(), payload.seatIds());
    }

    private SeatCommandPayload readPayload(MessageEnvelope envelope) {
        try {
            return objectMapper.readValue(envelope.payload(), SeatCommandPayload.class);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to parse " + envelope.eventType() + " payload for " + envelope.aggregateId(), e);
        }
    }

    private void publishEvent(String bookingId, String eventType, Object payload) {
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize " + eventType + " payload for " + bookingId, e);
        }
        outboxDao.insert(OutboxRecord.newRecord(
                "Booking", bookingId, Topics.INVENTORY_EVENTS, bookingId, eventType, payloadJson));
    }

    public record SeatCommandPayload(UUID showId, List<String> seatIds) {
    }

    private record SeatIdsPayload(List<String> seatIds) {
    }

    private record SeatIdPayload(String seatId) {
    }
}