package com.debankar.saga.inventory.kafka;

import com.debankar.saga.common.inbox.IdempotentConsumer;
import com.debankar.saga.common.inbox.InboxDao;
import com.debankar.saga.common.messaging.MessageEnvelope;
import com.debankar.saga.common.outbox.OutboxDao;
import com.debankar.saga.common.outbox.OutboxRecord;
import com.debankar.saga.inventory.seat.Seat;
import com.debankar.saga.inventory.seat.SeatDao;
import com.debankar.saga.inventory.seat.SeatHoldService;
import com.debankar.saga.inventory.seat.SeatStatus;
import com.debankar.saga.inventory.seat.Show;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class InventoryCommandListenerTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static HikariDataSource dataSource;
    static JdbcTemplate jdbcTemplate;
    static ObjectMapper objectMapper;

    @BeforeAll
    static void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        dataSource = new HikariDataSource(config);

        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        jdbcTemplate = new JdbcTemplate(dataSource);
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @AfterAll
    static void tearDown() {
        dataSource.close();
    }

    @AfterEach
    void cleanUp() {
        jdbcTemplate.update("DELETE FROM outbox_message");
        jdbcTemplate.update("DELETE FROM inbox_message");
    }

    private InventoryCommandListener newListener() {
        SeatHoldService seatHoldService = new SeatHoldService(
                jdbcTemplate, new DataSourceTransactionManager(dataSource), Duration.ofMinutes(5));
        IdempotentConsumer idempotentConsumer = new IdempotentConsumer(
                new InboxDao(jdbcTemplate), new TransactionTemplate(new DataSourceTransactionManager(dataSource)), new SimpleMeterRegistry());
        return new InventoryCommandListener(seatHoldService, idempotentConsumer, new OutboxDao(jdbcTemplate), objectMapper);
    }

    private UUID seedShowWithSeats(List<String> seatLabels) {
        UUID showId = UUID.randomUUID();
        SeatDao dao = new SeatDao(jdbcTemplate);
        dao.insertShow(new Show(showId, "Test Show", "Test Venue", Instant.now()));
        dao.insertSeats(showId, seatLabels);
        return showId;
    }

    private static String envelopeJson(UUID messageId, String eventType, String bookingId, Object payload) throws Exception {
        String payloadJson = objectMapper.writeValueAsString(payload);
        MessageEnvelope envelope =
                new MessageEnvelope(messageId, eventType, "Booking", bookingId, Instant.now(), payloadJson);
        return objectMapper.writeValueAsString(envelope);
    }

    private List<OutboxRecord> outboxRowsFor(String bookingId) {
        return new OutboxDao(jdbcTemplate).findUnpublished(50).stream()
                .filter(r -> r.aggregateId().equals(bookingId))
                .toList();
    }

    @Test
    void holdSeatsCommandHoldsTheSeatsAndPublishesSeatsHeld() throws Exception {
        UUID showId = seedShowWithSeats(List.of("A1", "A2"));
        String json = envelopeJson(UUID.randomUUID(), "HoldSeats", "booking-1",
                new InventoryCommandListener.SeatCommandPayload(showId, List.of("A1", "A2")));

        newListener().onCommand(json);

        List<Seat> seats = new SeatDao(jdbcTemplate).findByShow(showId);
        assertThat(seats).allSatisfy(seat -> {
            assertThat(seat.status()).isEqualTo(SeatStatus.HELD);
            assertThat(seat.bookingId()).isEqualTo("booking-1");
        });

        List<OutboxRecord> events = outboxRowsFor("booking-1");
        assertThat(events).hasSize(1);
        assertThat(events.get(0).eventType()).isEqualTo("SeatsHeld");
        assertThat(events.get(0).payload()).contains("A1").contains("A2");
    }

    @Test
    void holdSeatsCommandThatCannotBeSatisfiedPublishesSeatsHoldRejected() throws Exception {
        UUID showId = seedShowWithSeats(List.of("A1", "A2"));
        newListener().onCommand(envelopeJson(UUID.randomUUID(), "HoldSeats", "booking-1",
                new InventoryCommandListener.SeatCommandPayload(showId, List.of("A1"))));

        String json = envelopeJson(UUID.randomUUID(), "HoldSeats", "booking-2",
                new InventoryCommandListener.SeatCommandPayload(showId, List.of("A1", "A2")));
        newListener().onCommand(json);

        List<OutboxRecord> events = outboxRowsFor("booking-2");
        assertThat(events).hasSize(1);
        assertThat(events.get(0).eventType()).isEqualTo("SeatsHoldRejected");
        assertThat(events.get(0).payload()).contains("A1");
    }

    @Test
    void confirmSeatsCommandConfirmsHeldSeatsAndPublishesSeatsConfirmed() throws Exception {
        UUID showId = seedShowWithSeats(List.of("A1"));
        newListener().onCommand(envelopeJson(UUID.randomUUID(), "HoldSeats", "booking-1",
                new InventoryCommandListener.SeatCommandPayload(showId, List.of("A1"))));

        String json = envelopeJson(UUID.randomUUID(), "ConfirmSeats", "booking-1",
                new InventoryCommandListener.SeatCommandPayload(showId, List.of("A1")));
        newListener().onCommand(json);

        Seat seat = new SeatDao(jdbcTemplate).findByShow(showId).get(0);
        assertThat(seat.status()).isEqualTo(SeatStatus.BOOKED);

        List<OutboxRecord> confirmEvents = outboxRowsFor("booking-1").stream()
                .filter(r -> r.eventType().equals("SeatsConfirmed"))
                .toList();
        assertThat(confirmEvents).hasSize(1);
    }

    @Test
    void confirmSeatsCommandThatCannotBeSatisfiedRollsBackPartialWorkAndPublishesRejectionWithoutThrowing() throws Exception {
        UUID showId = seedShowWithSeats(List.of("A1", "A2"));
        newListener().onCommand(envelopeJson(UUID.randomUUID(), "HoldSeats", "booking-1",
                new InventoryCommandListener.SeatCommandPayload(showId, List.of("A1", "A2"))));

        // Simulate the expiry sweeper reclaiming A2 a moment before confirm runs.
        jdbcTemplate.update(
                """
                UPDATE seat SET status = 'AVAILABLE', booking_id = NULL, hold_expires_at = NULL
                WHERE show_id = ? AND seat_label = 'A2'
                """,
                showId);

        String json = envelopeJson(UUID.randomUUID(), "ConfirmSeats", "booking-1",
                new InventoryCommandListener.SeatCommandPayload(showId, List.of("A1", "A2")));

        // The point of this test: this must complete normally. The original
        // bug threw UnexpectedRollbackException right here, because
        // confirmSeats's own partial-write rollback (undoing A1's tentative
        // confirm) silently doomed the surrounding IdempotentConsumer
        // transaction, so by the time that transaction tried to commit the
        // SeatsConfirmRejected event below, Spring discovered it had already
        // been marked rollback-only.
        newListener().onCommand(json);

        Seat a1 = new SeatDao(jdbcTemplate).findByShow(showId).stream()
                .filter(seat -> seat.seatLabel().equals("A1"))
                .findFirst()
                .orElseThrow();
        assertThat(a1.status())
                .as("A1's tentative confirm must roll back to HELD, not commit as BOOKED")
                .isEqualTo(SeatStatus.HELD);

        List<OutboxRecord> events = outboxRowsFor("booking-1").stream()
                .filter(r -> r.eventType().equals("SeatsConfirmRejected"))
                .toList();
        assertThat(events).hasSize(1);
    }

    @Test
    void releaseSeatsCommandReleasesTheSeatsAndPublishesNoEvent() throws Exception {
        UUID showId = seedShowWithSeats(List.of("A1"));
        newListener().onCommand(envelopeJson(UUID.randomUUID(), "HoldSeats", "booking-1",
                new InventoryCommandListener.SeatCommandPayload(showId, List.of("A1"))));
        jdbcTemplate.update("DELETE FROM outbox_message"); // isolate what release itself publishes

        String json = envelopeJson(UUID.randomUUID(), "ReleaseSeats", "booking-1",
                new InventoryCommandListener.SeatCommandPayload(showId, List.of("A1")));
        newListener().onCommand(json);

        Seat seat = new SeatDao(jdbcTemplate).findByShow(showId).get(0);
        assertThat(seat.status()).isEqualTo(SeatStatus.AVAILABLE);
        assertThat(seat.bookingId()).isNull();
        assertThat(outboxRowsFor("booking-1")).isEmpty();
    }

    @Test
    void sameHoldSeatsMessageDeliveredTwiceAppliesItsEffectExactlyOnce() throws Exception {
        UUID showId = seedShowWithSeats(List.of("A1"));
        String json = envelopeJson(UUID.randomUUID(), "HoldSeats", "booking-1",
                new InventoryCommandListener.SeatCommandPayload(showId, List.of("A1")));

        InventoryCommandListener listener = newListener();
        listener.onCommand(json);
        listener.onCommand(json); // simulated redelivery, identical message

        Seat seat = new SeatDao(jdbcTemplate).findByShow(showId).get(0);
        assertThat(seat.status()).isEqualTo(SeatStatus.HELD);
        assertThat(seat.bookingId()).isEqualTo("booking-1");

        List<OutboxRecord> events = outboxRowsFor("booking-1");
        assertThat(events)
                .as("a redelivered command must not publish a second SeatsHeld event")
                .hasSize(1);
    }
}