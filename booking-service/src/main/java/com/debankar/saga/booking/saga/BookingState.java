package com.debankar.saga.booking.saga;

public enum BookingState {
    PENDING,
    HELD,
    PAID,
    CONFIRMED,
    CANCELLED,
    REJECTED;

    public boolean isFinal() {
        return this == CONFIRMED || this == CANCELLED || this == REJECTED;
    }
}