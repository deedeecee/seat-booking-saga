package com.debankar.saga.common.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class MessageEnvelopeTest {

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void roundTripsThroughJson() throws Exception {
        MessageEnvelope original = new MessageEnvelope(
                UUID.randomUUID(),
                "SeatsHeld",
                "Booking",
                "booking-123",
                Instant.parse("2026-09-22T10:15:30Z"),
                "{\"seatIds\":[\"A1\",\"A2\"]}"
        );

        String json = mapper.writeValueAsString(original);
        MessageEnvelope roundTripped = mapper.readValue(json, MessageEnvelope.class);

        assertThat(roundTripped).isEqualTo(original);
    }
}