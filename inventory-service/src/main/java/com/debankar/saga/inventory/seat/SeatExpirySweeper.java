package com.debankar.saga.inventory.seat;

import com.debankar.saga.common.Topics;
import com.debankar.saga.common.outbox.OutboxDao;
import com.debankar.saga.common.outbox.OutboxRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Finds seats whose hold expired without the booking confirming or being
 * cancelled some other way, and reclaims them.
 *
 * <p>Unlike {@link SeatHoldService#releaseSeats}, which only ever runs in
 * response to a command booking-service already sent (so booking-service
 * already knows the outcome), a sweep is the one place inventory discovers
 * an expiration on its own — nothing else will ever tell booking-service
 * the hold timed out. So each release here is paired with a {@code
 * HoldExpired} outbox event in the same transaction: one pair per expired
 * booking, each booking in its own transaction, so one booking's failure
 * can't roll back an unrelated booking's sweep in the same pass.
 */
public class SeatExpirySweeper {

    private final JdbcTemplate jdbcTemplate;
    private final OutboxDao outboxDao;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;

    public SeatExpirySweeper(JdbcTemplate jdbcTemplate,
                             OutboxDao outboxDao,
                             ObjectMapper objectMapper,
                             TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.outboxDao = outboxDao;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
    }

    /** Reclaims every currently expired hold and returns how many bookings were swept. */
    public int sweepExpiredHolds() {
        List<Seat> expired = new SeatDao(jdbcTemplate).findExpiredHolds(Instant.now());
        Map<BookingKey, List<String>> byBooking = groupByBooking(expired);

        int sweptCount = 0;
        for (Map.Entry<BookingKey, List<String>> entry : byBooking.entrySet()) {
            if (sweepOneBooking(entry.getKey(), entry.getValue())) {
                sweptCount++;
            }
        }
        return sweptCount;
    }

    private Map<BookingKey, List<String>> groupByBooking(List<Seat> expiredSeats) {
        Map<BookingKey, List<String>> byBooking = new LinkedHashMap<>();
        for (Seat seat : expiredSeats) {
            BookingKey key = new BookingKey(seat.showId(), seat.bookingId());
            byBooking.computeIfAbsent(key, k -> new ArrayList<>()).add(seat.seatLabel());
        }
        return byBooking;
    }

    private boolean sweepOneBooking(BookingKey key, List<String> seatLabels) {
        Boolean released = transactionTemplate.execute(status -> {
            int rows = 0;
            for (String label : seatLabels) {
                rows += jdbcTemplate.update(
                        """
                        UPDATE seat SET status = 'AVAILABLE', booking_id = NULL, hold_expires_at = NULL
                        WHERE show_id = ? AND seat_label = ? AND booking_id = ? AND status = 'HELD'
                              AND hold_expires_at < ?
                        """,
                        key.showId(), label, key.bookingId(), Timestamp.from(Instant.now())
                );
            }
            if (rows == 0) {
                // Already reclaimed by a concurrent sweep pass in the gap
                // between the find and here — nothing to report.
                return false;
            }

            publishHoldExpired(key, seatLabels);
            return true;
        });
        return Boolean.TRUE.equals(released);
    }

    private void publishHoldExpired(BookingKey key, List<String> seatLabels) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(new HoldExpiredPayload(seatLabels));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to serialize HoldExpired payload for booking " + key.bookingId(), e);
        }

        outboxDao.insert(OutboxRecord.newRecord(
                "Booking", key.bookingId(), Topics.INVENTORY_EVENTS, key.bookingId(), "HoldExpired", payload));
    }

    private record BookingKey(UUID showId, String bookingId) {
    }

    private record HoldExpiredPayload(List<String> seatLabels) {
    }
}