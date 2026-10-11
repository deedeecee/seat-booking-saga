package com.debankar.saga.booking.saga;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Finds bookings that have been sitting in a non-final state longer than
 * the configured timeout, with nothing having moved them at all, and
 * fires {@link SagaEvent#TIMEOUT} at each one through the exact same
 * {@link BookingOrchestrator} every real event already goes through — so
 * a reaped booking is compensated via the same code path, and the same
 * spec-table rows, a live {@code PaymentDeclined} or {@code HoldExpired}
 * would use.
 *
 * <p>Unlike {@code SeatExpirySweeper}, which discovers an expiration
 * nobody else will ever report, this sweeper's timeout is a deliberate
 * last resort: the default is set well past how long any legitimate,
 * successful attempt should ever take, so in ordinary operation a
 * {@code HoldExpired} or {@code PaymentDeclined} will already have moved
 * a stuck-looking booking along before this is ever reached. This only
 * fires for a genuinely lost message — a publish that silently failed,
 * or one that landed on a dead-letter topic instead of being redelivered.
 *
 * <p>No extra guarding is needed against a booking finishing in the gap
 * between being found here and {@link BookingOrchestrator#applyEvent}
 * actually running: that method re-reads the booking's current state
 * fresh before doing anything, and every final state already ignores
 * {@code TIMEOUT} by construction — proven exhaustively, for every state
 * and every event, by {@code BookingStateMachineTest}'s own no-op check.
 */
public class SagaTimeoutSweeper {

    private final BookingDao bookingDao;
    private final BookingOrchestrator bookingOrchestrator;
    private final Duration sagaTimeout;

    public SagaTimeoutSweeper(BookingDao bookingDao, BookingOrchestrator bookingOrchestrator, Duration sagaTimeout) {
        this.bookingDao = bookingDao;
        this.bookingOrchestrator = bookingOrchestrator;
        this.sagaTimeout = sagaTimeout;
    }

    /** Reaps every currently stuck booking and returns how many were reaped. */
    public int sweepStuckBookings() {
        List<Booking> stuck = bookingDao.findStuckBookings(Instant.now().minus(sagaTimeout));
        for (Booking booking : stuck) {
            bookingOrchestrator.applyEvent(booking.id(), SagaEvent.TIMEOUT);
        }
        return stuck.size();
    }
}