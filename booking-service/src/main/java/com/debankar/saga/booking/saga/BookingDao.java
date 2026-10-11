package com.debankar.saga.booking.saga;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Reads and writes the {@code booking} table. {@link #updateState} is
 * guarded by optimistic locking — the caller supplies the version it
 * read, and the update only takes effect if that version is still
 * current — so {@link BookingOrchestrator} can safely retry against fresh
 * data when two events for the same booking are applied at nearly the
 * same moment from different listener threads. {@link #insertIfAbsent}
 * follows the same insert-then-read-back shape already established by
 * {@code PaymentDao} and {@code InboxDao}: a booking's own id doubles as
 * its idempotency key, so a repeated creation attempt with the same id is
 * a no-op insert, not a new decision. {@link #findStuckBookings} backs
 * {@link SagaTimeoutSweeper} — it relies on {@code updated_at}, which
 * {@link #updateState} bumps on every successful transition, so a
 * booking only counts as stuck if nothing has moved it at all for the
 * full timeout window, not merely that it hasn't reached a final state
 * yet.
 */
public class BookingDao {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public BookingDao(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    /** Plain insert, used directly only by this class's own tests to seed fixtures. */
    public void insert(Booking booking) {
        jdbcTemplate.update(
                """
                INSERT INTO booking (id, show_id, seat_ids, amount_cents, state, version)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                booking.id(), booking.showId(), writeSeatIds(booking.seatIds()), booking.amountCents(),
                booking.state().name(), booking.version()
        );
    }

    /** Returns true if this call created the row, false if a booking with this id already existed. */
    public boolean insertIfAbsent(Booking booking) {
        int rows = jdbcTemplate.update(
                """
                INSERT INTO booking (id, show_id, seat_ids, amount_cents, state, version)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO NOTHING
                """,
                booking.id(), booking.showId(), writeSeatIds(booking.seatIds()), booking.amountCents(),
                booking.state().name(), booking.version()
        );
        return rows == 1;
    }

    public Optional<Booking> findById(String id) {
        return jdbcTemplate.query(
                """
                SELECT id, show_id, seat_ids, amount_cents, state, version, created_at, updated_at
                FROM booking WHERE id = ?
                """,
                this::mapRow,
                id
        ).stream().findFirst();
    }

    /** Returns {@code true} if {@code expectedVersion} was still current and the update took effect. */
    public boolean updateState(String id, long expectedVersion, BookingState newState) {
        int rows = jdbcTemplate.update(
                "UPDATE booking SET state = ?, version = version + 1, updated_at = now() WHERE id = ? AND version = ?",
                newState.name(), id, expectedVersion
        );
        return rows == 1;
    }

    /** Every booking still in a non-final state whose last update is older than {@code olderThan}. */
    public List<Booking> findStuckBookings(Instant olderThan) {
        return jdbcTemplate.query(
                """
                SELECT id, show_id, seat_ids, amount_cents, state, version, created_at, updated_at
                FROM booking
                WHERE state NOT IN ('CONFIRMED', 'CANCELLED', 'REJECTED') AND updated_at < ?
                """,
                this::mapRow,
                Timestamp.from(olderThan)
        );
    }

    private Booking mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new Booking(
                rs.getString("id"),
                rs.getObject("show_id", UUID.class),
                readSeatIds(rs.getString("seat_ids")),
                rs.getLong("amount_cents"),
                BookingState.valueOf(rs.getString("state")),
                rs.getLong("version"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant()
        );
    }

    private String writeSeatIds(List<String> seatIds) {
        try {
            return objectMapper.writeValueAsString(seatIds);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize seatIds", e);
        }
    }

    private List<String> readSeatIds(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<List<String>>() {
            });
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize seatIds", e);
        }
    }
}