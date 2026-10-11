package com.debankar.saga.booking.saga;

/**
 * Every inbound trigger the state machine reacts to. The first six are
 * Kafka events from inventory or payment; TIMEOUT is not a Kafka message
 * at all — it's raised locally, by the saga-timeout sweeper built in a
 * later step, for a booking that's been sitting in a non-final state too
 * long with no event arriving to move it along.
 */
public enum SagaEvent {
    SEATS_HELD("SeatsHeld"),
    SEATS_HOLD_REJECTED("SeatsHoldRejected"),
    PAYMENT_AUTHORIZED("PaymentAuthorized"),
    PAYMENT_DECLINED("PaymentDeclined"),
    SEATS_CONFIRMED("SeatsConfirmed"),
    SEATS_CONFIRM_REJECTED("SeatsConfirmRejected"),
    HOLD_EXPIRED("HoldExpired"),
    TIMEOUT("Timeout");

    private final String wireName;

    SagaEvent(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }

    public static SagaEvent fromWireName(String wireName) {
        for (SagaEvent event : values()) {
            if (event.wireName.equals(wireName)) {
                return event;
            }
        }
        throw new IllegalArgumentException("Unrecognized saga event type: " + wireName);
    }
}