package com.debankar.saga.booking.saga;

import com.debankar.saga.common.outbox.OutboxDao;
import com.debankar.saga.common.outbox.OutboxRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Applies one saga event to one booking: looks up its current state, asks
 * {@link BookingStateMachine} what should happen, and — if anything
 * actually changes — persists the new state and publishes every resulting
 * command through the outbox, all as one unit of work. {@link
 * #createIfAbsent} is the one exception: a new booking has no previous
 * state to transition from, so it doesn't go through the state machine at
 * all — it just creates the row as {@code PENDING} and, only on a genuine
 * first creation, publishes the saga's opening command. Both paths share
 * {@link #publishCommand}, so how an outbound command actually gets built
 * and written to the outbox lives in exactly one place.
 *
 * <p>Uses {@link TransactionDefinition#PROPAGATION_NESTED}, as a blanket
 * policy for every domain service the saga composes inside {@code
 * IdempotentConsumer}'s transaction — see {@code SeatHoldService}'s own
 * Javadoc for why. Nothing in this class's current logic actually needs
 * a savepoint, since every path here either commits cleanly or throws
 * outright, but committing to {@code NESTED} everywhere, rather than only
 * where today's logic happens to require it, means a future change to
 * this class can't silently reintroduce that bug by accident.
 *
 * <p>An optimistic-lock miss on {@link BookingDao#updateState} — another
 * writer advanced the row between this method's read and its write — is
 * retried against a fresh read, not treated as a failure: two events for
 * the same booking can legitimately arrive from different listener
 * threads at nearly the same moment, since inventory.events and
 * payment.events are separate topics with no ordering guarantee between
 * them. Only exhausting the retry budget under sustained contention is
 * treated as a genuine error and allowed to propagate — unlike an
 * ordinary saga outcome like a declined payment, that really is
 * abnormal, and is meant to surface through the retry and dead-letter
 * handling rather than being silently swallowed.
 */
public class BookingOrchestrator {

    private static final int MAX_OPTIMISTIC_LOCK_RETRIES = 5;

    private final BookingDao bookingDao;
    private final OutboxDao outboxDao;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;

    public BookingOrchestrator(BookingDao bookingDao,
                               OutboxDao outboxDao,
                               ObjectMapper objectMapper,
                               PlatformTransactionManager transactionManager) {
        this.bookingDao = bookingDao;
        this.outboxDao = outboxDao;
        this.objectMapper = objectMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
    }

    /**
     * Creates a new booking as {@code PENDING} and publishes {@code
     * HoldSeats}, or — if {@code bookingId} already exists — returns the
     * existing booking untouched and publishes nothing. {@code bookingId}
     * is expected to be the client-supplied idempotency key, so this is
     * what makes a repeated creation request with the same key safe to
     * call any number of times.
     */
    public Booking createIfAbsent(String bookingId, UUID showId, List<String> seatIds, long amountCents) {
        return transactionTemplate.execute(status -> {
            Booking booking = new Booking(bookingId, showId, seatIds, amountCents,
                    BookingState.PENDING, 0, Instant.now(), Instant.now());
            boolean created = bookingDao.insertIfAbsent(booking);
            if (created) {
                publishCommand(booking, SagaCommand.HOLD_SEATS);
            }
            return bookingDao.findById(bookingId).orElseThrow();
        });
    }

    /**
     * Applies {@code event} to the booking identified by {@code bookingId}.
     * Does nothing, quietly, if that booking doesn't exist, or if the
     * state machine treats this event as a no-op for the booking's
     * current state — including every ordinary duplicate or out-of-order
     * delivery, which is indistinguishable, by design, from any other
     * no-op here.
     */
    public void applyEvent(String bookingId, SagaEvent event) {
        transactionTemplate.executeWithoutResult(status -> applyWithRetry(bookingId, event, 0));
    }

    private void applyWithRetry(String bookingId, SagaEvent event, int attempt) {
        Optional<Booking> maybeBooking = bookingDao.findById(bookingId);
        if (maybeBooking.isEmpty()) {
            return;
        }
        Booking booking = maybeBooking.get();

        Transition transition = BookingStateMachine.apply(booking.state(), event);
        if (transition.nextState() == booking.state() && transition.commandsToEmit().isEmpty()) {
            return;
        }

        boolean updated = bookingDao.updateState(bookingId, booking.version(), transition.nextState());
        if (!updated) {
            if (attempt >= MAX_OPTIMISTIC_LOCK_RETRIES) {
                throw new IllegalStateException(
                        "Exhausted optimistic-lock retries applying " + event + " to booking " + bookingId);
            }
            applyWithRetry(bookingId, event, attempt + 1);
            return;
        }

        for (SagaCommand command : transition.commandsToEmit()) {
            publishCommand(booking, command);
        }
    }

    private void publishCommand(Booking booking, SagaCommand command) {
        Object payload = switch (command) {
            case HOLD_SEATS, CONFIRM_SEATS, RELEASE_SEATS ->
                    new SeatCommandPayload(booking.showId(), booking.seatIds());
            case AUTHORIZE_PAYMENT -> new AuthorizePaymentPayload(booking.amountCents());
            case VOID_PAYMENT -> Map.of();
        };

        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to serialize " + command + " payload for booking " + booking.id(), e);
        }

        outboxDao.insert(OutboxRecord.newRecord(
                "Booking", booking.id(), command.topic(), booking.id(), command.wireName(), payloadJson));
    }

    /**
     * Deliberately independent of inventory-service's and payment-service's
     * own payload records, even though the JSON shape must match by
     * agreement: each service owns its own view of the messages it sends
     * and receives, not a shared Java type reaching across a service
     * boundary.
     */
    private record SeatCommandPayload(UUID showId, List<String> seatIds) {
    }

    private record AuthorizePaymentPayload(long amountCents) {
    }
}