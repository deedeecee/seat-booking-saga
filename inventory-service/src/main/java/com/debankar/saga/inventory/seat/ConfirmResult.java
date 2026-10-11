package com.debankar.saga.inventory.seat;

import java.util.List;

public sealed interface ConfirmResult {

    record Confirmed(List<String> seatLabels) implements ConfirmResult {
    }

    /**
     * {@code requestedSeatLabels} echoes back what was asked for, not which
     * specific seat failed — a caller that needs to know confirm failed at
     * all, to trigger compensation, doesn't need a finer breakdown, and the
     * seat table itself always reflects ground truth.
     */
    record Rejected(List<String> requestedSeatLabels) implements ConfirmResult {
    }
}