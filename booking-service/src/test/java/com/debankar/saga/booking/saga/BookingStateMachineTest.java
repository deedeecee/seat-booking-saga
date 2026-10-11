package com.debankar.saga.booking.saga;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class BookingStateMachineTest {

    /**
     * One row per line of the saga's own spec table. Every state/event
     * combination the saga explicitly defines is listed here — this is
     * the table-driven proof the roadmap's own gate for this part asks
     * for, directly.
     */
    static Stream<Arguments> specTableTransitions() {
        return Stream.of(
                Arguments.of(BookingState.PENDING, SagaEvent.SEATS_HELD,
                        BookingState.HELD, List.of(SagaCommand.AUTHORIZE_PAYMENT)),
                Arguments.of(BookingState.PENDING, SagaEvent.SEATS_HOLD_REJECTED,
                        BookingState.REJECTED, List.<SagaCommand>of()),
                Arguments.of(BookingState.PENDING, SagaEvent.TIMEOUT,
                        BookingState.CANCELLED, List.of(SagaCommand.RELEASE_SEATS, SagaCommand.VOID_PAYMENT)),

                Arguments.of(BookingState.HELD, SagaEvent.PAYMENT_AUTHORIZED,
                        BookingState.PAID, List.of(SagaCommand.CONFIRM_SEATS)),
                Arguments.of(BookingState.HELD, SagaEvent.PAYMENT_DECLINED,
                        BookingState.CANCELLED, List.of(SagaCommand.RELEASE_SEATS)),
                Arguments.of(BookingState.HELD, SagaEvent.HOLD_EXPIRED,
                        BookingState.CANCELLED, List.<SagaCommand>of()),
                Arguments.of(BookingState.HELD, SagaEvent.TIMEOUT,
                        BookingState.CANCELLED, List.of(SagaCommand.RELEASE_SEATS, SagaCommand.VOID_PAYMENT)),

                Arguments.of(BookingState.PAID, SagaEvent.SEATS_CONFIRMED,
                        BookingState.CONFIRMED, List.<SagaCommand>of()),
                Arguments.of(BookingState.PAID, SagaEvent.SEATS_CONFIRM_REJECTED,
                        BookingState.CANCELLED, List.of(SagaCommand.VOID_PAYMENT)),
                Arguments.of(BookingState.PAID, SagaEvent.HOLD_EXPIRED,
                        BookingState.CANCELLED, List.of(SagaCommand.VOID_PAYMENT)),
                Arguments.of(BookingState.PAID, SagaEvent.TIMEOUT,
                        BookingState.CANCELLED, List.of(SagaCommand.RELEASE_SEATS, SagaCommand.VOID_PAYMENT)),

                Arguments.of(BookingState.CANCELLED, SagaEvent.PAYMENT_AUTHORIZED,
                        BookingState.CANCELLED, List.of(SagaCommand.VOID_PAYMENT)),
                Arguments.of(BookingState.CANCELLED, SagaEvent.SEATS_HELD,
                        BookingState.CANCELLED, List.of(SagaCommand.RELEASE_SEATS))
        );
    }

    @ParameterizedTest(name = "{0} + {1} -> {2}, emits {3}")
    @MethodSource("specTableTransitions")
    void everySpecTableRowProducesExactlyItsDefinedTransition(
            BookingState currentState, SagaEvent event, BookingState expectedNextState,
            List<SagaCommand> expectedCommands) {
        Transition result = BookingStateMachine.apply(currentState, event);

        assertThat(result.nextState()).isEqualTo(expectedNextState);
        assertThat(result.commandsToEmit()).isEqualTo(expectedCommands);
    }

    @Test
    void everyCombinationNotInTheSpecTableIsANoOp() {
        for (BookingState state : BookingState.values()) {
            for (SagaEvent event : SagaEvent.values()) {
                if (isInSpecTable(state, event)) {
                    continue;
                }
                Transition result = BookingStateMachine.apply(state, event);
                assertThat(result.nextState())
                        .as("%s + %s should not change state", state, event)
                        .isEqualTo(state);
                assertThat(result.commandsToEmit())
                        .as("%s + %s should emit no commands", state, event)
                        .isEmpty();
            }
        }
    }

    private boolean isInSpecTable(BookingState state, SagaEvent event) {
        return specTableTransitions()
                .anyMatch(args -> args.get()[0] == state && args.get()[1] == event);
    }
}