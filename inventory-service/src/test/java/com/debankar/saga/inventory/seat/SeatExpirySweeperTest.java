package com.debankar.saga.inventory.seat;

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
import org.springframework.transaction.support.TransactionTemplate;
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
class SeatExpirySweeperTest {

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

    private SeatExpirySweeper newSweeper() {
        TransactionTemplate transactionTemplate =
                new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        return new SeatExpirySweeper(jdbcTemplate, new OutboxDao(jdbcTemplate), objectMapper, transactionTemplate);
    }

    private UUID seedShowWithSeats(List<String> seatLabels) {
        UUID showId = UUID.randomUUID();
        SeatDao dao = new SeatDao(jdbcTemplate);
        dao.insertShow(new Show(showId, "Test Show", "Test Venue", Instant.now()));
        dao.insertSeats(showId, seatLabels);
        return showId;
    }

    private void holdWithExpiry(UUID showId, String bookingId, List<String> seatLabels, Instant expiresAt) {
        new SeatHoldService(jdbcTemplate, new DataSourceTransactionManager(dataSource), Duration.ofMinutes(5))
                .holdSeats(showId, bookingId, seatLabels);
        for (String label : seatLabels) {
            jdbcTemplate.update(
                    "UPDATE seat SET hold_expires_at = ? WHERE show_id = ? AND seat_label = ?",
                    Timestamp.from(expiresAt), showId, label);
        }
    }

    private List<OutboxRecord> findOutboxRowsFor(String bookingId) {
        return new OutboxDao(jdbcTemplate).findUnpublished(50).stream()
                .filter(r -> r.aggregateId().equals(bookingId))
                .toList();
    }

    @Test
    void sweepsAnExpiredHoldReleasesTheSeatAndEmitsHoldExpired() {
        UUID showId = seedShowWithSeats(List.of("A1"));
        holdWithExpiry(showId, "booking-1", List.of("A1"), Instant.now().minusSeconds(5));

        int swept = newSweeper().sweepExpiredHolds();

        assertThat(swept).isEqualTo(1);
        Seat seat = new SeatDao(jdbcTemplate).findByShow(showId).get(0);
        assertThat(seat.status()).isEqualTo(SeatStatus.AVAILABLE);
        assertThat(seat.bookingId()).isNull();

        List<OutboxRecord> rows = findOutboxRowsFor("booking-1");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).eventType()).isEqualTo("HoldExpired");
        assertThat(rows.get(0).payload()).contains("A1");
    }

    @Test
    void leavesAHoldThatHasNotExpiredYetUntouched() {
        UUID showId = seedShowWithSeats(List.of("A1"));
        holdWithExpiry(showId, "booking-1", List.of("A1"), Instant.now().plusSeconds(300));

        int swept = newSweeper().sweepExpiredHolds();

        assertThat(swept).isZero();
        Seat seat = new SeatDao(jdbcTemplate).findByShow(showId).get(0);
        assertThat(seat.status()).isEqualTo(SeatStatus.HELD);
        assertThat(findOutboxRowsFor("booking-1")).isEmpty();
    }

    @Test
    void sweepsMultipleSeatsForTheSameBookingAsOneOutboxEvent() {
        UUID showId = seedShowWithSeats(List.of("A1", "A2"));
        holdWithExpiry(showId, "booking-1", List.of("A1", "A2"), Instant.now().minusSeconds(5));

        int swept = newSweeper().sweepExpiredHolds();

        assertThat(swept).isEqualTo(1);
        List<Seat> seats = new SeatDao(jdbcTemplate).findByShow(showId);
        assertThat(seats).allSatisfy(seat -> assertThat(seat.status()).isEqualTo(SeatStatus.AVAILABLE));

        List<OutboxRecord> rows = findOutboxRowsFor("booking-1");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).payload()).contains("A1").contains("A2");
    }

    @Test
    void sweepsDifferentExpiredBookingsIndependently() {
        UUID showId = seedShowWithSeats(List.of("A1", "B1"));
        holdWithExpiry(showId, "booking-1", List.of("A1"), Instant.now().minusSeconds(5));
        holdWithExpiry(showId, "booking-2", List.of("B1"), Instant.now().minusSeconds(5));

        int swept = newSweeper().sweepExpiredHolds();

        assertThat(swept).isEqualTo(2);
        assertThat(findOutboxRowsFor("booking-1")).hasSize(1);
        assertThat(findOutboxRowsFor("booking-2")).hasSize(1);
    }
}