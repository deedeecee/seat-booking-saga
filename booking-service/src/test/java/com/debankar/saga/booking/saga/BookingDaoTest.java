package com.debankar.saga.booking.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class BookingDaoTest {

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

    private BookingDao newDao() {
        return new BookingDao(jdbcTemplate, objectMapper);
    }

    private Booking newBooking(String id) {
        return new Booking(id, UUID.randomUUID(), List.of("A1", "A2"), 5000,
                BookingState.PENDING, 0, Instant.now(), Instant.now());
    }

    @Test
    void insertedBookingIsReadableWithSeatIdsIntact() {
        BookingDao dao = newDao();
        Booking booking = newBooking("booking-1");

        dao.insert(booking);
        Booking found = dao.findById("booking-1").orElseThrow();

        assertThat(found.id()).isEqualTo("booking-1");
        assertThat(found.showId()).isEqualTo(booking.showId());
        assertThat(found.seatIds()).containsExactly("A1", "A2");
        assertThat(found.amountCents()).isEqualTo(5000);
        assertThat(found.state()).isEqualTo(BookingState.PENDING);
        assertThat(found.version()).isEqualTo(0);
    }

    @Test
    void findByIdReturnsEmptyForAnUnknownBooking() {
        Optional<Booking> found = newDao().findById("no-such-booking");
        assertThat(found).isEmpty();
    }

    @Test
    void updateStateWithTheCorrectVersionSucceedsAndBumpsVersion() {
        BookingDao dao = newDao();
        dao.insert(newBooking("booking-2"));

        boolean updated = dao.updateState("booking-2", 0, BookingState.HELD);

        assertThat(updated).isTrue();
        Booking found = dao.findById("booking-2").orElseThrow();
        assertThat(found.state()).isEqualTo(BookingState.HELD);
        assertThat(found.version()).isEqualTo(1);
    }

    @Test
    void updateStateWithAStaleVersionFailsAndLeavesTheRowUntouched() {
        BookingDao dao = newDao();
        dao.insert(newBooking("booking-3"));
        dao.updateState("booking-3", 0, BookingState.HELD); // version is now 1

        boolean updated = dao.updateState("booking-3", 0, BookingState.CANCELLED); // stale: still passing 0

        assertThat(updated).isFalse();
        Booking found = dao.findById("booking-3").orElseThrow();
        assertThat(found.state())
                .as("a stale-version update must not apply, even though it asked for a valid next state")
                .isEqualTo(BookingState.HELD);
        assertThat(found.version()).isEqualTo(1);
    }

    @Test
    void insertIfAbsentReturnsTrueOnFirstInsertAndFalseOnASecondAttempt() {
        BookingDao dao = newDao();
        Booking booking = newBooking("booking-4");

        boolean firstInsert = dao.insertIfAbsent(booking);
        boolean secondInsert = dao.insertIfAbsent(booking);

        assertThat(firstInsert).isTrue();
        assertThat(secondInsert).isFalse();
        assertThat(dao.findById("booking-4")).isPresent();
    }
}