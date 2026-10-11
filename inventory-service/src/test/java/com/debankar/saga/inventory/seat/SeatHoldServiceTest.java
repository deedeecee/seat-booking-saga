package com.debankar.saga.inventory.seat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class SeatHoldServiceTest {

    // Postgres' default max_connections (100) is too low for the 200-thread
    // test below to run as genuinely concurrent database work rather than
    // queueing behind the limit, so it's raised for this container only.
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine")
            .withCommand("postgres", "-c", "max_connections=300");

    static HikariDataSource dataSource;
    static JdbcTemplate jdbcTemplate;

    @BeforeAll
    static void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        config.setMaximumPoolSize(220);
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

    private SeatHoldService newService() {
        return new SeatHoldService(jdbcTemplate, new DataSourceTransactionManager(dataSource), Duration.ofMinutes(5));
    }

    private UUID seedShowWithSeats(List<String> seatLabels) {
        UUID showId = UUID.randomUUID();
        SeatDao dao = new SeatDao(jdbcTemplate);
        dao.insertShow(new Show(showId, "Test Show", "Test Venue", Instant.now()));
        dao.insertSeats(showId, seatLabels);
        return showId;
    }

    @Test
    void holdingAvailableSeatsSucceedsAndSetsBookingAndExpiry() {
        UUID showId = seedShowWithSeats(List.of("A1", "A2"));

        HoldResult result = newService().holdSeats(showId, "booking-1", List.of("A1", "A2"));

        assertThat(result).isInstanceOf(HoldResult.Held.class);
        List<Seat> seats = new SeatDao(jdbcTemplate).findByShow(showId);
        assertThat(seats).allSatisfy(seat -> {
            assertThat(seat.status()).isEqualTo(SeatStatus.HELD);
            assertThat(seat.bookingId()).isEqualTo("booking-1");
            assertThat(seat.holdExpiresAt()).isAfter(Instant.now());
        });
    }

    @Test
    void rejectsAllOrNothingWhenOneRequestedSeatIsAlreadyHeld() {
        UUID showId = seedShowWithSeats(List.of("A1", "A2"));
        HoldResult first = newService().holdSeats(showId, "booking-1", List.of("A1"));
        assertThat(first).isInstanceOf(HoldResult.Held.class);

        HoldResult second = newService().holdSeats(showId, "booking-2", List.of("A1", "A2"));

        assertThat(second).isInstanceOf(HoldResult.Rejected.class);
        Seat a2 = new SeatDao(jdbcTemplate).findByShow(showId).stream()
                .filter(seat -> seat.seatLabel().equals("A2"))
                .findFirst()
                .orElseThrow();
        assertThat(a2.status())
                .as("A2 was available but must stay untouched since the hold was all-or-nothing")
                .isEqualTo(SeatStatus.AVAILABLE);
    }

    @Test
    void twoHundredThreadsRaceForOneSeatExactlyOneWins() throws Exception {
        UUID showId = seedShowWithSeats(List.of("Z1"));
        int threadCount = 200;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);

        try {
            List<Future<HoldResult>> futures = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                String bookingId = "booking-" + i;
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    return newService().holdSeats(showId, bookingId, List.of("Z1"));
                }));
            }
            assertThat(ready.await(15, TimeUnit.SECONDS)).as("all threads should reach the start line").isTrue();
            start.countDown();

            long heldCount = 0;
            for (Future<HoldResult> future : futures) {
                if (future.get(30, TimeUnit.SECONDS) instanceof HoldResult.Held) {
                    heldCount++;
                }
            }

            assertThat(heldCount).as("exactly one of 200 racing holds should win").isEqualTo(1);

            List<Seat> seats = new SeatDao(jdbcTemplate).findByShow(showId);
            assertThat(seats).hasSize(1);
            assertThat(seats.get(0).status()).isEqualTo(SeatStatus.HELD);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void overlappingMultiSeatRequestsNeverDeadlockOrHoldPartially() throws Exception {
        UUID showId = seedShowWithSeats(List.of("Z1", "Z2"));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        try {
            Future<HoldResult> resultA = executor.submit(() -> {
                ready.countDown();
                start.await();
                return newService().holdSeats(showId, "booking-A", List.of("Z1", "Z2"));
            });
            Future<HoldResult> resultB = executor.submit(() -> {
                ready.countDown();
                start.await();
                // Deliberately reversed order: proves the lock order comes
                // from SeatHoldService's own sort, not from request order.
                return newService().holdSeats(showId, "booking-B", List.of("Z2", "Z1"));
            });

            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            HoldResult a = resultA.get(15, TimeUnit.SECONDS);
            HoldResult b = resultB.get(15, TimeUnit.SECONDS);

            boolean exactlyOneWon = (a instanceof HoldResult.Held) ^ (b instanceof HoldResult.Held);
            assertThat(exactlyOneWon)
                    .as("exactly one booking should hold both seats; A=%s B=%s", a, b)
                    .isTrue();

            List<Seat> seats = new SeatDao(jdbcTemplate).findByShow(showId);
            String winner = seats.get(0).bookingId();
            assertThat(seats).allSatisfy(seat -> {
                assertThat(seat.status()).isEqualTo(SeatStatus.HELD);
                assertThat(seat.bookingId())
                        .as("both seats must end up with the same booking — never split")
                        .isEqualTo(winner);
            });
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void confirmingMovesHeldSeatsToBookedAndClearsExpiry() {
        UUID showId = seedShowWithSeats(List.of("A1", "A2"));
        newService().holdSeats(showId, "booking-1", List.of("A1", "A2"));

        ConfirmResult result = newService().confirmSeats(showId, "booking-1", List.of("A1", "A2"));

        assertThat(result).isInstanceOf(ConfirmResult.Confirmed.class);
        List<Seat> seats = new SeatDao(jdbcTemplate).findByShow(showId);
        assertThat(seats).allSatisfy(seat -> {
            assertThat(seat.status()).isEqualTo(SeatStatus.BOOKED);
            assertThat(seat.bookingId()).isEqualTo("booking-1");
            assertThat(seat.holdExpiresAt()).isNull();
        });
    }

    @Test
    void confirmingRejectsAllOrNothingWhenOneSeatWasReclaimedFirst() {
        UUID showId = seedShowWithSeats(List.of("A1", "A2"));
        HoldResult hold = newService().holdSeats(showId, "booking-1", List.of("A1", "A2"));
        assertThat(hold).isInstanceOf(HoldResult.Held.class);

        // Simulate the expiry sweeper (part D) reclaiming A2 a moment
        // before confirm runs, without needing real time to pass.
        jdbcTemplate.update(
                """
                UPDATE seat SET status = 'AVAILABLE', booking_id = NULL, hold_expires_at = NULL
                WHERE show_id = ? AND seat_label = 'A2'
                """,
                showId);

        ConfirmResult result = newService().confirmSeats(showId, "booking-1", List.of("A1", "A2"));

        assertThat(result).isInstanceOf(ConfirmResult.Rejected.class);
        Seat a1 = new SeatDao(jdbcTemplate).findByShow(showId).stream()
                .filter(seat -> seat.seatLabel().equals("A1"))
                .findFirst()
                .orElseThrow();
        assertThat(a1.status())
                .as("A1 was still legitimately held but must roll back since A2 couldn't confirm")
                .isEqualTo(SeatStatus.HELD);
    }

    @Test
    void releasingMovesHeldSeatsBackToAvailable() {
        UUID showId = seedShowWithSeats(List.of("A1", "A2"));
        newService().holdSeats(showId, "booking-1", List.of("A1", "A2"));

        ReleaseResult result = newService().releaseSeats(showId, "booking-1", List.of("A1", "A2"));

        assertThat(result.releasedSeatLabels()).containsExactlyInAnyOrder("A1", "A2");
        List<Seat> seats = new SeatDao(jdbcTemplate).findByShow(showId);
        assertThat(seats).allSatisfy(seat -> {
            assertThat(seat.status()).isEqualTo(SeatStatus.AVAILABLE);
            assertThat(seat.bookingId()).isNull();
            assertThat(seat.holdExpiresAt()).isNull();
        });
    }

    @Test
    void releasingTheSameBookingTwiceIsASafeNoOpTheSecondTime() {
        UUID showId = seedShowWithSeats(List.of("A1"));
        newService().holdSeats(showId, "booking-1", List.of("A1"));

        ReleaseResult first = newService().releaseSeats(showId, "booking-1", List.of("A1"));
        ReleaseResult second = newService().releaseSeats(showId, "booking-1", List.of("A1"));

        assertThat(first.releasedSeatLabels()).containsExactly("A1");
        assertThat(second.releasedSeatLabels()).isEmpty();
    }
}