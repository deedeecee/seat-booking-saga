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

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class SagaTimeoutSweeperTest {

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

    private SagaTimeoutSweeper newSweeper(Duration sagaTimeout) {
        BookingOrchestrator orchestrator = new BookingOrchestrator(
                new BookingDao(jdbcTemplate, objectMapper), new OutboxDao(jdbcTemplate), objectMapper,
                new DataSourceTransactionManager(dataSource));
        return new SagaTimeoutSweeper(new BookingDao(jdbcTemplate, objectMapper), orchestrator, sagaTimeout);
    }

    private void seedBookingWithUpdatedAt(String id, BookingState state, Instant updatedAt) {
        Booking booking = new Booking(id, UUID.randomUUID(), List.of("A1"), 5000, state, 0,
                Instant.now(), Instant.now());
        new BookingDao(jdbcTemplate, objectMapper).insert(booking);
        jdbcTemplate.update("UPDATE booking SET updated_at = ? WHERE id = ?", Timestamp.from(updatedAt), id);
    }

    private List<OutboxRecord> outboxRowsFor(String bookingId) {
        return new OutboxDao(jdbcTemplate).findUnpublished(50).stream()
                .filter(r -> r.aggregateId().equals(bookingId))
                .toList();
    }

    @Test
    void reapsABookingStuckPastTheTimeout() {
        String bookingId = "booking-" + UUID.randomUUID();
        seedBookingWithUpdatedAt(bookingId, BookingState.HELD, Instant.now().minusSeconds(120));

        int swept = newSweeper(Duration.ofSeconds(60)).sweepStuckBookings();

        assertThat(swept).isEqualTo(1);
        Booking booking = new BookingDao(jdbcTemplate, objectMapper).findById(bookingId).orElseThrow();
        assertThat(booking.state()).isEqualTo(BookingState.CANCELLED);

        List<OutboxRecord> events = outboxRowsFor(bookingId);
        assertThat(events).extracting(OutboxRecord::eventType)
                .containsExactlyInAnyOrder("ReleaseSeats", "VoidPayment");
    }

    @Test
    void leavesABookingThatHasNotYetTimedOutUntouched() {
        String bookingId = "booking-" + UUID.randomUUID();
        seedBookingWithUpdatedAt(bookingId, BookingState.HELD, Instant.now().minusSeconds(5));

        int swept = newSweeper(Duration.ofSeconds(60)).sweepStuckBookings();

        assertThat(swept).isZero();
        Booking booking = new BookingDao(jdbcTemplate, objectMapper).findById(bookingId).orElseThrow();
        assertThat(booking.state()).isEqualTo(BookingState.HELD);
    }

    @Test
    void neverTouchesABookingAlreadyInAFinalState() {
        String bookingId = "booking-" + UUID.randomUUID();
        // Deliberately old updated_at on an already-CONFIRMED booking: proves
        // the WHERE clause's state filter, not just timing, keeps this safe.
        seedBookingWithUpdatedAt(bookingId, BookingState.CONFIRMED, Instant.now().minusSeconds(600));

        int swept = newSweeper(Duration.ofSeconds(60)).sweepStuckBookings();

        assertThat(swept).isZero();
        assertThat(outboxRowsFor(bookingId)).isEmpty();
    }

    @Test
    void reapsMultipleStuckBookingsIndependently() {
        String bookingId1 = "booking-" + UUID.randomUUID();
        String bookingId2 = "booking-" + UUID.randomUUID();
        seedBookingWithUpdatedAt(bookingId1, BookingState.PENDING, Instant.now().minusSeconds(120));
        seedBookingWithUpdatedAt(bookingId2, BookingState.PAID, Instant.now().minusSeconds(120));

        int swept = newSweeper(Duration.ofSeconds(60)).sweepStuckBookings();

        assertThat(swept).isEqualTo(2);
        BookingDao dao = new BookingDao(jdbcTemplate, objectMapper);
        assertThat(dao.findById(bookingId1).orElseThrow().state()).isEqualTo(BookingState.CANCELLED);
        assertThat(dao.findById(bookingId2).orElseThrow().state()).isEqualTo(BookingState.CANCELLED);
        assertThat(outboxRowsFor(bookingId1)).hasSize(2); // PENDING+TIMEOUT: ReleaseSeats + VoidPayment
        assertThat(outboxRowsFor(bookingId2)).hasSize(2); // PAID+TIMEOUT: ReleaseSeats + VoidPayment
    }
}