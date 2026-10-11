package com.debankar.saga.inventory.seat;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Reads and writes the {@code show} and {@code seat} tables. Seeding is a
 * plain insert with no idempotency guarantee — a show is created exactly
 * once by whoever seeds it, not redelivered through Kafka the way the
 * saga's own commands and events are.
 */
public class SeatDao {

    private static final RowMapper<Seat> SEAT_ROW_MAPPER = (rs, rowNum) -> new Seat(
            rs.getObject("show_id", UUID.class),
            rs.getString("seat_label"),
            SeatStatus.valueOf(rs.getString("status")),
            rs.getString("booking_id"),
            rs.getTimestamp("hold_expires_at") == null
                    ? null : rs.getTimestamp("hold_expires_at").toInstant()
    );

    private final JdbcTemplate jdbcTemplate;

    public SeatDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insertShow(Show show) {
        jdbcTemplate.update(
                "INSERT INTO show (id, title, venue, starts_at) VALUES (?, ?, ?, ?)",
                show.id(), show.title(), show.venue(), Timestamp.from(show.startsAt())
        );
    }

    public void insertSeats(UUID showId, List<String> seatLabels) {
        List<Object[]> batchArgs = seatLabels.stream()
                .map(label -> new Object[]{showId, label})
                .toList();
        jdbcTemplate.batchUpdate(
                "INSERT INTO seat (show_id, seat_label) VALUES (?, ?)",
                batchArgs
        );
    }

    public List<Seat> findByShow(UUID showId) {
        return jdbcTemplate.query(
                """
                SELECT show_id, seat_label, status, booking_id, hold_expires_at
                FROM seat WHERE show_id = ? ORDER BY seat_label
                """,
                SEAT_ROW_MAPPER,
                showId
        );
    }

    /**
     * Every seat still {@code HELD} past its expiry, across every show —
     * the sweeper groups these by booking itself. Backed by {@code
     * idx_seat_held_expiry}, the partial index built for exactly this
     * query.
     */
    public List<Seat> findExpiredHolds(Instant asOf) {
        return jdbcTemplate.query(
                """
                SELECT show_id, seat_label, status, booking_id, hold_expires_at
                FROM seat
                WHERE status = 'HELD' AND hold_expires_at < ?
                ORDER BY show_id, booking_id, seat_label
                """,
                SEAT_ROW_MAPPER,
                Timestamp.from(asOf)
        );
    }
}