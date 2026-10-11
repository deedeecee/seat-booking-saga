package com.debankar.saga.payment.kafka;

import com.debankar.saga.common.Topics;
import com.debankar.saga.common.inbox.IdempotentConsumer;
import com.debankar.saga.common.messaging.MessageEnvelope;
import com.debankar.saga.common.outbox.OutboxDao;
import com.debankar.saga.common.outbox.OutboxRecord;
import com.debankar.saga.payment.gateway.AuthorizeResult;
import com.debankar.saga.payment.gateway.PaymentService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Consumes every command on {@code payment.commands} and dispatches by
 * {@code eventType}: {@code AuthorizePayment} and {@code VoidPayment} are
 * multiplexed onto one topic, matching how {@code InventoryCommandListener}
 * handles inventory's commands, for the same reasons — see that class's
 * own Javadoc for why each handler runs inside {@code IdempotentConsumer}'s
 * transaction rather than opening one of its own, and why a malformed
 * envelope or payload is left to propagate straight out of the listener
 * method into the retry-then-dead-letter handling.
 */
public class PaymentCommandListener {

    private final PaymentService paymentService;
    private final IdempotentConsumer idempotentConsumer;
    private final OutboxDao outboxDao;
    private final ObjectMapper objectMapper;

    public PaymentCommandListener(PaymentService paymentService,
                                  IdempotentConsumer idempotentConsumer,
                                  OutboxDao outboxDao,
                                  ObjectMapper objectMapper) {
        this.paymentService = paymentService;
        this.idempotentConsumer = idempotentConsumer;
        this.outboxDao = outboxDao;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.PAYMENT_COMMANDS, groupId = "payment-service")
    public void onCommand(String rawEnvelope) throws Exception {
        MessageEnvelope envelope = objectMapper.readValue(rawEnvelope, MessageEnvelope.class);

        switch (envelope.eventType()) {
            case "AuthorizePayment" -> idempotentConsumer.processOnce(envelope, () -> handleAuthorize(envelope));
            case "VoidPayment" -> idempotentConsumer.processOnce(envelope, () -> handleVoid(envelope));
            default -> throw new IllegalArgumentException(
                    "Unrecognized payment command event type: " + envelope.eventType());
        }
    }

    private void handleAuthorize(MessageEnvelope envelope) {
        AuthorizePaymentPayload payload = readPayload(envelope, AuthorizePaymentPayload.class);
        AuthorizeResult result = paymentService.authorize(envelope.aggregateId(), payload.amountCents());

        if (result instanceof AuthorizeResult.Authorized authorized) {
            publishEvent(envelope.aggregateId(), "PaymentAuthorized",
                    new PaymentAuthorizedPayload(authorized.amountCents()));
        } else if (result instanceof AuthorizeResult.Declined declined) {
            publishEvent(envelope.aggregateId(), "PaymentDeclined", new PaymentDeclinedPayload(declined.reason()));
        }
    }

    private void handleVoid(MessageEnvelope envelope) {
        // Fire-and-forget compensation, matching ReleaseSeats: nothing in
        // the saga waits for a reply to VoidPayment, so no event is published.
        paymentService.voidPayment(envelope.aggregateId());
    }

    private <T> T readPayload(MessageEnvelope envelope, Class<T> type) {
        try {
            return objectMapper.readValue(envelope.payload(), type);
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
                "Booking", bookingId, Topics.PAYMENT_EVENTS, bookingId, eventType, payloadJson));
    }

    public record AuthorizePaymentPayload(long amountCents) {
    }

    private record PaymentAuthorizedPayload(long amountCents) {
    }

    private record PaymentDeclinedPayload(String reason) {
    }
}