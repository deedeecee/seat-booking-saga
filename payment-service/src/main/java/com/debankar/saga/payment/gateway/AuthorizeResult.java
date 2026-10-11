package com.debankar.saga.payment.gateway;

public sealed interface AuthorizeResult {

    record Authorized(long amountCents) implements AuthorizeResult {
    }

    record Declined(String reason) implements AuthorizeResult {
    }
}