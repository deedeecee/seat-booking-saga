package com.debankar.saga.booking.saga;

import com.debankar.saga.common.Topics;

public enum SagaCommand {
    HOLD_SEATS("HoldSeats", Topics.INVENTORY_COMMANDS),
    CONFIRM_SEATS("ConfirmSeats", Topics.INVENTORY_COMMANDS),
    RELEASE_SEATS("ReleaseSeats", Topics.INVENTORY_COMMANDS),
    AUTHORIZE_PAYMENT("AuthorizePayment", Topics.PAYMENT_COMMANDS),
    VOID_PAYMENT("VoidPayment", Topics.PAYMENT_COMMANDS);

    private final String wireName;
    private final String topic;

    SagaCommand(String wireName, String topic) {
        this.wireName = wireName;
        this.topic = topic;
    }

    public String wireName() {
        return wireName;
    }

    public String topic() {
        return topic;
    }
}