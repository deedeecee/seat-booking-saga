package com.debankar.saga.inventory.seat;

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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class SeatDaoTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static HikariDataSource dataSource;
    static JdbcTemplate jdbcTemplate;

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
    }

    @AfterAll
    static void tearDown() {
        dataSource.close();
    }

    private SeatDao newDao() {
        return new SeatDao(jdbcTemplate);
    }

    @Test
    void seedsAShowWithItsSeatsAndReadsThemBackAvailable() {
        SeatDao dao = newDao();
        UUID showId = UUID.randomUUID();

        dao.insertShow(new Show(showId, "Test Show", "Test Venue", Instant.now()));
        dao.insertSeats(showId, List.of("A1", "A2", "A3"));

        List<Seat> seats = dao.findByShow(showId);

        assertThat(seats).hasSize(3);
        assertThat(seats).extracting(Seat::seatLabel).containsExactly("A1", "A2", "A3");
        assertThat(seats).allSatisfy(seat -> {
            assertThat(seat.status()).isEqualTo(SeatStatus.AVAILABLE);
            assertThat(seat.bookingId()).isNull();
            assertThat(seat.holdExpiresAt()).isNull();
        });
    }

    @Test
    void seatsFromDifferentShowsDoNotMix() {
        SeatDao dao = newDao();
        UUID showA = UUID.randomUUID();
        UUID showB = UUID.randomUUID();

        dao.insertShow(new Show(showA, "Show A", "Venue A", Instant.now()));
        dao.insertSeats(showA, List.of("A1"));
        dao.insertShow(new Show(showB, "Show B", "Venue B", Instant.now()));
        dao.insertSeats(showB, List.of("A1", "B1"));

        assertThat(dao.findByShow(showA)).hasSize(1);
        assertThat(dao.findByShow(showB)).hasSize(2);
    }
}