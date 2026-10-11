package com.debankar.saga.common.consistency;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class ConsistencyCheckerTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine")
            .withInitScript("consistency-checker-init.sql");

    static HikariDataSource bookingDataSource;
    static HikariDataSource inventoryDataSource;
    static HikariDataSource paymentDataSource;
    static JdbcTemplate bookingJdbc;
    static JdbcTemplate inventoryJdbc;
    static JdbcTemplate paymentJdbc;

    @BeforeAll
    static void setUp() {
        bookingDataSource = dataSourceFor("booking_db");
        inventoryDataSource = dataSourceFor("inventory_db");
        paymentDataSource = dataSourceFor("payment_db");
        bookingJdbc = new JdbcTemplate(bookingDataSource);
        inventoryJdbc = new JdbcTemplate(inventoryDataSource);
        paymentJdbc = new JdbcTemplate(paymentDataSource);

        bookingJdbc.execute("""
                CREATE TABLE booking (
                    id VARCHAR(100) PRIMARY KEY,
                    show_id UUID NOT NULL,
                    seat_ids TEXT NOT NULL,
                    amount_cents BIGINT NOT NULL,
                    state VARCHAR(20) NOT NULL,
                    version BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);

        inventoryJdbc.execute("""
                CREATE TABLE seat (
                    show_id UUID NOT NULL,
                    seat_label VARCHAR(10) NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'AVAILABLE',
                    booking_id VARCHAR(100),
                    hold_expires_at TIMESTAMPTZ,
                    PRIMARY KEY (show_id, seat_label)
                )
                """);

        paymentJdbc.execute("""
                CREATE TABLE payment (
                    booking_id VARCHAR(100) PRIMARY KEY,
                    status VARCHAR(20) NOT NULL,
                    amount_cents BIGINT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);
    }

    @AfterAll
    static void tearDown() {
        bookingDataSource.close();
        inventoryDataSource.close();
        paymentDataSource.close();
    }

    @AfterEach
    void cleanUpTables() {
        bookingJdbc.execute("TRUNCATE booking");
        inventoryJdbc.execute("TRUNCATE seat");
        paymentJdbc.execute("TRUNCATE payment");
    }

    private static HikariDataSource dataSourceFor(String database) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + POSTGRES.getHost() + ":" + POSTGRES.getFirstMappedPort() + "/" + database);
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        return new HikariDataSource(config);
    }

    private ConsistencyChecker newChecker() {
        return new ConsistencyChecker(bookingJdbc, inventoryJdbc, paymentJdbc);
    }

    private void insertBooking(String id, String showId, String seatIdsJson, String state, Instant updatedAt) {
        bookingJdbc.update(
                "INSERT INTO booking (id, show_id, seat_ids, amount_cents, state, updated_at) VALUES (?, ?::uuid, ?, 5000, ?, ?)",
                id, showId, seatIdsJson, state, Timestamp.from(updatedAt));
    }

    private void insertSeat(String showId, String seatLabel, String status, String bookingId) {
        inventoryJdbc.update(
                "INSERT INTO seat (show_id, seat_label, status, booking_id) VALUES (?::uuid, ?, ?, ?)",
                showId, seatLabel, status, bookingId);
    }

    private void insertPayment(String bookingId, String status) {
        paymentJdbc.update(
                "INSERT INTO payment (booking_id, status, amount_cents) VALUES (?, ?, 5000)",
                bookingId, status);
    }

    @Test
    void validSagaOutcomesProduceNoViolations() {
        String showId = "11111111-1111-1111-1111-111111111111";

        insertBooking("confirmed-1", showId, "[\"A1\"]", "CONFIRMED", Instant.now());
        insertSeat(showId, "A1", "BOOKED", "confirmed-1");
        insertPayment("confirmed-1", "AUTHORIZED");

        insertBooking("cancelled-1", showId, "[\"A2\"]", "CANCELLED", Instant.now());
        insertSeat(showId, "A2", "AVAILABLE", null);
        insertPayment("cancelled-1", "VOIDED");

        insertBooking("held-1", showId, "[\"A3\"]", "HELD", Instant.now());
        insertSeat(showId, "A3", "HELD", "held-1");

        ConsistencyReport report = newChecker().check(Duration.ofMinutes(5));

        assertThat(report.isClean()).as("violations: %s", report.violations()).isTrue();
    }

    @Test
    void detectsASeatHeldByABookingThatDoesNotClaimIt() {
        String showId = "22222222-2222-2222-2222-222222222222";
        insertBooking("booking-1", showId, "[\"A1\"]", "HELD", Instant.now());
        insertSeat(showId, "A1", "HELD", "booking-1");
        // Corruption: seat A2 is also attributed to booking-1, which never asked for it.
        insertSeat(showId, "A2", "HELD", "booking-1");

        ConsistencyReport report = newChecker().check(Duration.ofMinutes(5));

        assertThat(report.violations()).extracting(Violation::invariant).contains("no-seat-in-two-bookings");
    }

    @Test
    void detectsAConfirmedBookingWithNoAuthorizedPayment() {
        String showId = "33333333-3333-3333-3333-333333333333";
        insertBooking("booking-2", showId, "[\"A1\"]", "CONFIRMED", Instant.now());
        insertSeat(showId, "A1", "BOOKED", "booking-2");
        // Corruption: no payment row at all for a confirmed booking.

        ConsistencyReport report = newChecker().check(Duration.ofMinutes(5));

        assertThat(report.violations()).extracting(Violation::invariant).contains("confirmed-needs-authorized-payment");
    }

    @Test
    void detectsACancelledBookingThatStillHoldsASeat() {
        String showId = "44444444-4444-4444-4444-444444444444";
        insertBooking("booking-3", showId, "[\"A1\"]", "CANCELLED", Instant.now());
        // Corruption: the seat was never actually released.
        insertSeat(showId, "A1", "HELD", "booking-3");

        ConsistencyReport report = newChecker().check(Duration.ofMinutes(5));

        assertThat(report.violations()).extracting(Violation::invariant).contains("dead-booking-holds-nothing");
    }

    @Test
    void detectsARejectedBookingWithAStillAuthorizedPayment() {
        String showId = "55555555-5555-5555-5555-555555555555";
        insertBooking("booking-4", showId, "[\"A1\"]", "REJECTED", Instant.now());
        // Corruption: payment was authorized but never voided for a rejected booking.
        insertPayment("booking-4", "AUTHORIZED");

        ConsistencyReport report = newChecker().check(Duration.ofMinutes(5));

        assertThat(report.violations()).extracting(Violation::invariant).contains("dead-booking-has-no-live-authorization");
    }

    @Test
    void detectsANonFinalBookingOlderThanTheThreshold() {
        String showId = "66666666-6666-6666-6666-666666666666";
        // Corruption: stuck in HELD far past any reasonable threshold —
        // simulating the reaper itself having failed.
        insertBooking("booking-5", showId, "[\"A1\"]", "HELD", Instant.now().minus(Duration.ofHours(1)));
        insertSeat(showId, "A1", "HELD", "booking-5");

        ConsistencyReport report = newChecker().check(Duration.ofMinutes(5));

        assertThat(report.violations()).extracting(Violation::invariant).contains("no-stuck-non-final-booking");
    }
}