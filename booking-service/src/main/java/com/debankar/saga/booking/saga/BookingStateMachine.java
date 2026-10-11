package com.debankar.saga.booking.saga;

import java.util.List;

/**
 * The saga's entire transition table, as pure, dependency-free logic.
 * Everything that actually moves a booking — persistence, publishing the
 * resulting commands — is built on top of this in a later part; this
 * class only ever needs a state and an event to produce an answer.
 *
 * <p>Any {@code (state, event)} pair not explicitly listed below falls
 * through to {@link Transition#noOp}: same state, no commands. This is
 * the state machine's own defense against both a duplicate delivery of
 * an event already acted on, and a genuinely out-of-order arrival — and
 * it's a different, complementary layer from {@code IdempotentConsumer}'s
 * inbox table, which dedupes by message id. Two distinct outbox rows that
 * happen to carry the same logical event (a bug, or a redelivery path
 * that doesn't preserve the original message id) would each get past the
 * inbox check as "new," but still can't move the state machine anywhere
 * it hasn't already been told to go.
 *
 * <p>The {@code CANCELLED} case explicitly handles two different late
 * arrivals, not one: a {@code PaymentAuthorized} reaching an
 * already-cancelled booking voids the money; a {@code SeatsHeld} doing
 * the same releases the seat. Both are genuinely possible — a command
 * sent before cancellation can always be fulfilled by its receiving
 * service after the booking has already moved on, since nothing stops
 * inventory or payment from succeeding at a request that's since become
 * moot. Without the second case, a late hold would sit unreleased
 * forever, since nothing else in the saga ever revisits an already-held
 * seat belonging to a finished booking.
 */
public final class BookingStateMachine {

    private BookingStateMachine() {
    }

    public static Transition apply(BookingState currentState, SagaEvent event) {
        return switch (currentState) {
            case PENDING -> switch (event) {
                case SEATS_HELD -> new Transition(BookingState.HELD, List.of(SagaCommand.AUTHORIZE_PAYMENT));
                case SEATS_HOLD_REJECTED -> new Transition(BookingState.REJECTED, List.of());
                case TIMEOUT -> new Transition(BookingState.CANCELLED,
                        List.of(SagaCommand.RELEASE_SEATS, SagaCommand.VOID_PAYMENT));
                default -> Transition.noOp(currentState);
            };
            case HELD -> switch (event) {
                case PAYMENT_AUTHORIZED -> new Transition(BookingState.PAID, List.of(SagaCommand.CONFIRM_SEATS));
                case PAYMENT_DECLINED -> new Transition(BookingState.CANCELLED, List.of(SagaCommand.RELEASE_SEATS));
                case HOLD_EXPIRED -> new Transition(BookingState.CANCELLED, List.of());
                case TIMEOUT -> new Transition(BookingState.CANCELLED,
                        List.of(SagaCommand.RELEASE_SEATS, SagaCommand.VOID_PAYMENT));
                default -> Transition.noOp(currentState);
            };
            case PAID -> switch (event) {
                case SEATS_CONFIRMED -> new Transition(BookingState.CONFIRMED, List.of());
                case SEATS_CONFIRM_REJECTED -> new Transition(BookingState.CANCELLED, List.of(SagaCommand.VOID_PAYMENT));
                case HOLD_EXPIRED -> new Transition(BookingState.CANCELLED, List.of(SagaCommand.VOID_PAYMENT));
                case TIMEOUT -> new Transition(BookingState.CANCELLED,
                        List.of(SagaCommand.RELEASE_SEATS, SagaCommand.VOID_PAYMENT));
                default -> Transition.noOp(currentState);
            };
            case CANCELLED -> switch (event) {
                case PAYMENT_AUTHORIZED -> new Transition(BookingState.CANCELLED, List.of(SagaCommand.VOID_PAYMENT));
                case SEATS_HELD -> new Transition(BookingState.CANCELLED, List.of(SagaCommand.RELEASE_SEATS));
                default -> Transition.noOp(currentState);
            };
            case CONFIRMED, REJECTED -> Transition.noOp(currentState);
        };
    }
}