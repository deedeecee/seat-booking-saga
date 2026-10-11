package com.debankar.saga.booking.saga;

import java.util.List;

public record Transition(BookingState nextState, List<SagaCommand> commandsToEmit) {

    public static Transition noOp(BookingState currentState) {
        return new Transition(currentState, List.of());
    }
}