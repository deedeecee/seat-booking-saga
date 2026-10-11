package com.debankar.saga.booking.saga;

import com.debankar.saga.common.outbox.OutboxDao;
import com.debankar.saga.common.outbox.OutboxRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class BookingOrchestratorTest {

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
    void cleanUpOutbox() {
        jdbcTemplate.update("DELETE FROM outbox_message");
    }

    private BookingOrchestrator newOrchestrator() {
        return new BookingOrchestrator(new BookingDao(jdbcTemplate, objectMapper), new OutboxDao(jdbcTemplate),
                objectMapper, new DataSourceTransactionManager(dataSource));
    }

    private String seedBooking(BookingState state) {
        String id = "booking-" + UUID.randomUUID();
        Booking booking = new Booking(id, UUID.randomUUID(), List.of("A1", "A2"), 5000,
                state, 0, Instant.now(), Instant.now());
        new BookingDao(jdbcTemplate, objectMapper).insert(booking);
        return id;
    }

    private List<OutboxRecord> outboxRowsFor(String bookingId) {
        return new OutboxDao(jdbcTemplate).findUnpublished(50).stream()
                .filter(r -> r.aggregateId().equals(bookingId))
                .toList();
    }

    @Test
    void validEventAdvancesStateAndPublishesItsCommand() {
        String bookingId = seedBooking(BookingState.PENDING);

        newOrchestrator().applyEvent(bookingId, SagaEvent.SEATS_HELD);

        Booking booking = new BookingDao(jdbcTemplate, objectMapper).findById(bookingId).orElseThrow();
        assertThat(booking.state()).isEqualTo(BookingState.HELD);
        assertThat(booking.version()).isEqualTo(1);

        List<OutboxRecord> events = outboxRowsFor(bookingId);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).eventType()).isEqualTo("AuthorizePayment");
        assertThat(events.get(0).payload()).contains("5000");
    }

    @Test
    void multiCommandTransitionPublishesBothCommands() {
        String bookingId = seedBooking(BookingState.HELD);

        newOrchestrator().applyEvent(bookingId, SagaEvent.TIMEOUT);

        Booking booking = new BookingDao(jdbcTemplate, objectMapper).findById(bookingId).orElseThrow();
        assertThat(booking.state()).isEqualTo(BookingState.CANCELLED);

        List<OutboxRecord> events = outboxRowsFor(bookingId);
        assertThat(events).extracting(OutboxRecord::eventType)
                .containsExactlyInAnyOrder("ReleaseSeats", "VoidPayment");
    }

    @Test
    void unhandledEventLeavesTheBookingCompletelyUntouched() {
        String bookingId = seedBooking(BookingState.PENDING);

        // PENDING has no defined reaction to PAYMENT_AUTHORIZED.
        newOrchestrator().applyEvent(bookingId, SagaEvent.PAYMENT_AUTHORIZED);

        Booking booking = new BookingDao(jdbcTemplate, objectMapper).findById(bookingId).orElseThrow();
        assertThat(booking.state()).isEqualTo(BookingState.PENDING);
        assertThat(booking.version())
                .as("a no-op event must not even bump the version")
                .isEqualTo(0);
        assertThat(outboxRowsFor(bookingId)).isEmpty();
    }

    @Test
    void applyingAnEventToAnUnknownBookingDoesNothingAndDoesNotThrow() {
        newOrchestrator().applyEvent("no-such-booking", SagaEvent.SEATS_HELD);
        // No exception is the assertion; there's nothing else to check.
    }

    @Test
    void twoEventsRacingOnTheSameBookingBothApplyWithoutErrorAndConverge() throws Exception {
        String bookingId = seedBooking(BookingState.HELD);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        try {
            // HOLD_EXPIRED and PAYMENT_DECLINED both move a HELD booking to
            // CANCELLED, so the outcome converges regardless of which one
            // wins the race for the first write — this only needs to prove
            // neither call errors and the version-guarded retry resolves
            // the contention cleanly, not force a specific winner.
            Future<?> resultA = executor.submit(() -> {
                ready.countDown();
                await(start);
                newOrchestrator().applyEvent(bookingId, SagaEvent.HOLD_EXPIRED);
                return null;
            });
            Future<?> resultB = executor.submit(() -> {
                ready.countDown();
                await(start);
                newOrchestrator().applyEvent(bookingId, SagaEvent.PAYMENT_DECLINED);
                return null;
            });

            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            resultA.get(15, TimeUnit.SECONDS);
            resultB.get(15, TimeUnit.SECONDS);

            Booking booking = new BookingDao(jdbcTemplate, objectMapper).findById(bookingId).orElseThrow();
            assertThat(booking.state()).isEqualTo(BookingState.CANCELLED);

            List<OutboxRecord> events = outboxRowsFor(bookingId);
            assertThat(events)
                    .as("only whichever event actually won the race against HELD should have published anything")
                    .hasSizeLessThanOrEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void creatingWithTheSameBookingIdTwicePublishesHoldSeatsOnlyOnce() {
        String bookingId = "booking-" + UUID.randomUUID();
        UUID showId = UUID.randomUUID();
        BookingOrchestrator orchestrator = newOrchestrator();

        Booking first = orchestrator.createIfAbsent(bookingId, showId, List.of("A1", "A2"), 5000);
        Booking second = orchestrator.createIfAbsent(bookingId, showId, List.of("A1", "A2"), 5000);

        assertThat(first.id()).isEqualTo(bookingId);
        assertThat(second.id()).isEqualTo(bookingId);
        assertThat(second.state()).isEqualTo(BookingState.PENDING);

        List<OutboxRecord> events = outboxRowsFor(bookingId);
        assertThat(events)
                .as("a repeated creation with the same booking id must not re-publish HoldSeats")
                .hasSize(1);
        assertThat(events.get(0).eventType()).isEqualTo("HoldSeats");
    }

    @Test
    void creatingANewBookingPublishesHoldSeatsWithTheGivenShowAndSeats() {
        String bookingId = "booking-" + UUID.randomUUID();
        UUID showId = UUID.randomUUID();

        Booking created = newOrchestrator().createIfAbsent(bookingId, showId, List.of("Z1"), 1500);

        assertThat(created.state()).isEqualTo(BookingState.PENDING);
        List<OutboxRecord> events = outboxRowsFor(bookingId);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).eventType()).isEqualTo("HoldSeats");
        assertThat(events.get(0).payload()).contains(showId.toString()).contains("Z1");
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}