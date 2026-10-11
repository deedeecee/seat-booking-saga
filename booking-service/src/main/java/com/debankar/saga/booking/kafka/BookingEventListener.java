package com.debankar.saga.booking.kafka;

import com.debankar.saga.booking.saga.BookingOrchestrator;
import com.debankar.saga.booking.saga.SagaEvent;
import com.debankar.saga.common.Topics;
import com.debankar.saga.common.inbox.IdempotentConsumer;
import com.debankar.saga.common.messaging.MessageEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Consumes both event topics the saga reacts to — inventory.events and
 * payment.events — and drives every message through {@link
 * BookingOrchestrator#applyEvent}, which is where the actual saga logic
 * lives. Both methods share one dispatch path deliberately: unlike {@code
 * InventoryCommandListener} or {@code PaymentCommandListener}, which call
 * a different handler per command because each one means different
 * business logic, every recognized event here ends up at the exact same
 * call — the branching already lives in {@code BookingStateMachine}, not
 * in this class. A wire-level {@code eventType} with no matching {@link
 * SagaEvent} throws before {@code IdempotentConsumer}'s transaction ever
 * opens, the same as the {@code default} arm of the other two listeners'
 * switch statements, and for the same reason: it propagates straight into
 * the retry-then-dead-letter handling.
 */
public class BookingEventListener {

    private final BookingOrchestrator bookingOrchestrator;
    private final IdempotentConsumer idempotentConsumer;
    private final ObjectMapper objectMapper;

    public BookingEventListener(BookingOrchestrator bookingOrchestrator,
                                IdempotentConsumer idempotentConsumer,
                                ObjectMapper objectMapper) {
        this.bookingOrchestrator = bookingOrchestrator;
        this.idempotentConsumer = idempotentConsumer;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.INVENTORY_EVENTS, groupId = "booking-service")
    public void onInventoryEvent(String rawEnvelope) throws Exception {
        dispatch(rawEnvelope);
    }

    @KafkaListener(topics = Topics.PAYMENT_EVENTS, groupId = "booking-service")
    public void onPaymentEvent(String rawEnvelope) throws Exception {
        dispatch(rawEnvelope);
    }

    private void dispatch(String rawEnvelope) throws Exception {
        MessageEnvelope envelope = objectMapper.readValue(rawEnvelope, MessageEnvelope.class);
        SagaEvent event = SagaEvent.fromWireName(envelope.eventType());
        idempotentConsumer.processOnce(envelope, () -> bookingOrchestrator.applyEvent(envelope.aggregateId(), event));
    }
}