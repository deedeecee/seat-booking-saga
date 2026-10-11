package com.debankar.saga.inventory.seat;

import java.util.List;

public sealed interface HoldResult {

    record Held(List<String> seatLabels) implements HoldResult {
    }

    /**
     * {@code firstUnavailableSeatLabel} is the first seat found unavailable
     * in sorted order, not an exhaustive list of every unavailable seat in
     * the request. {@link SeatHoldService} stops locking as soon as it finds
     * one, rather than locking the rest of an already-doomed request — that
     * would just make it hold up other, unrelated valid holds on seats it
     * doesn't even need.
     */
    record Rejected(String firstUnavailableSeatLabel) implements HoldResult {
    }
}