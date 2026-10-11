package com.debankar.saga.common.consistency;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Checks the four cross-database invariants the saga as a whole must
 * always hold, by querying booking_db, inventory_db, and payment_db
 * independently and cross-referencing the results here — Postgres has no
 * way to join across separate databases directly, and this project
 * deliberately keeps each service's database private to that service, so
 * there's no other way to ask these questions.
 *
 * <p>Each invariant maps onto a real failure mode a bug in this saga
 * could actually produce. The first and third form a bidirectional check
 * between inventory's own seat ownership and booking's own record of
 * what it claims to hold: the first catches inventory attributing a seat
 * to a booking that never actually asked for it; the third catches a
 * cancelled or rejected booking that still has something held in its
 * name. The second catches a confirmed booking whose payment or seats
 * didn't actually follow through. The fourth catches the saga-timeout
 * reaper itself having silently stopped working, using a threshold the
 * caller supplies — deliberately looser than the reaper's own configured
 * timeout, so this check never races the reaper it's meant to be
 * watching over.
 */
public class ConsistencyChecker {

    private final JdbcTemplate bookingJdbc;
    private final JdbcTemplate inventoryJdbc;
    private final JdbcTemplate paymentJdbc;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ConsistencyChecker(JdbcTemplate bookingJdbc, JdbcTemplate inventoryJdbc, JdbcTemplate paymentJdbc) {
        this.bookingJdbc = bookingJdbc;
        this.inventoryJdbc = inventoryJdbc;
        this.paymentJdbc = paymentJdbc;
    }

    public ConsistencyReport check(Duration maxNonFinalAge) {
        List<BookingRow> bookings = loadBookings();
        List<SeatRow> seats = loadSeats();
        Map<String, String> paymentStatusByBookingId = loadPaymentStatuses();

        List<Violation> violations = new ArrayList<>();
        checkNoSeatHeldByABookingThatDoesNotClaimIt(bookings, seats, violations);
        checkConfirmedBookingsHaveAuthorizedPaymentAndBookedSeats(bookings, seats, paymentStatusByBookingId, violations);
        checkCancelledOrRejectedBookingsHoldNothingAndHaveNoLiveAuthorization(
                bookings, seats, paymentStatusByBookingId, violations);
        checkNoNonFinalBookingOlderThanThreshold(bookings, maxNonFinalAge, violations);

        return new ConsistencyReport(violations);
    }

    private void checkNoSeatHeldByABookingThatDoesNotClaimIt(List<BookingRow> bookings, List<SeatRow> seats,
                                                             List<Violation> violations) {
        Map<String, Set<String>> seatIdsByBooking = new HashMap<>();
        for (BookingRow booking : bookings) {
            seatIdsByBooking.put(booking.id(), Set.copyOf(booking.seatIds()));
        }
        for (SeatRow seat : seats) {
            if (seat.bookingId() == null || !("HELD".equals(seat.status()) || "BOOKED".equals(seat.status()))) {
                continue;
            }
            Set<String> claimed = seatIdsByBooking.get(seat.bookingId());
            if (claimed == null || !claimed.contains(seat.seatLabel())) {
                violations.add(new Violation("no-seat-in-two-bookings",
                        "seat %s/%s is %s for booking %s, but that booking doesn't list this seat".formatted(
                                seat.showId(), seat.seatLabel(), seat.status(), seat.bookingId())));
            }
        }
    }

    private void checkConfirmedBookingsHaveAuthorizedPaymentAndBookedSeats(
            List<BookingRow> bookings, List<SeatRow> seats, Map<String, String> paymentStatusByBookingId,
            List<Violation> violations) {
        Map<String, SeatRow> seatByShowAndLabel = new HashMap<>();
        for (SeatRow seat : seats) {
            seatByShowAndLabel.put(seat.showId() + "/" + seat.seatLabel(), seat);
        }
        for (BookingRow booking : bookings) {
            if (!"CONFIRMED".equals(booking.state())) {
                continue;
            }
            if (!"AUTHORIZED".equals(paymentStatusByBookingId.get(booking.id()))) {
                violations.add(new Violation("confirmed-needs-authorized-payment",
                        "booking %s is CONFIRMED but has no AUTHORIZED payment".formatted(booking.id())));
            }
            for (String seatLabel : booking.seatIds()) {
                SeatRow seat = seatByShowAndLabel.get(booking.showId() + "/" + seatLabel);
                boolean bookedForThisBooking = seat != null && "BOOKED".equals(seat.status())
                        && booking.id().equals(seat.bookingId());
                if (!bookedForThisBooking) {
                    violations.add(new Violation("confirmed-needs-booked-seats",
                            "booking %s is CONFIRMED but seat %s/%s is not BOOKED for it".formatted(
                                    booking.id(), booking.showId(), seatLabel)));
                }
            }
        }
    }

    private void checkCancelledOrRejectedBookingsHoldNothingAndHaveNoLiveAuthorization(
            List<BookingRow> bookings, List<SeatRow> seats, Map<String, String> paymentStatusByBookingId,
            List<Violation> violations) {
        for (BookingRow booking : bookings) {
            if (!("CANCELLED".equals(booking.state()) || "REJECTED".equals(booking.state()))) {
                continue;
            }
            for (SeatRow seat : seats) {
                boolean stillHeldByThisBooking = booking.id().equals(seat.bookingId())
                        && ("HELD".equals(seat.status()) || "BOOKED".equals(seat.status()));
                if (stillHeldByThisBooking) {
                    violations.add(new Violation("dead-booking-holds-nothing",
                            "booking %s is %s but still holds seat %s/%s".formatted(
                                    booking.id(), booking.state(), seat.showId(), seat.seatLabel())));
                }
            }
            if ("AUTHORIZED".equals(paymentStatusByBookingId.get(booking.id()))) {
                violations.add(new Violation("dead-booking-has-no-live-authorization",
                        "booking %s is %s but still has an AUTHORIZED payment".formatted(
                                booking.id(), booking.state())));
            }
        }
    }

    private void checkNoNonFinalBookingOlderThanThreshold(List<BookingRow> bookings, Duration maxNonFinalAge,
                                                          List<Violation> violations) {
        Instant threshold = Instant.now().minus(maxNonFinalAge);
        for (BookingRow booking : bookings) {
            boolean isFinal = "CONFIRMED".equals(booking.state()) || "CANCELLED".equals(booking.state())
                    || "REJECTED".equals(booking.state());
            if (!isFinal && booking.updatedAt().isBefore(threshold)) {
                violations.add(new Violation("no-stuck-non-final-booking",
                        "booking %s has been %s since %s, past the %s threshold".formatted(
                                booking.id(), booking.state(), booking.updatedAt(), maxNonFinalAge)));
            }
        }
    }

    private List<BookingRow> loadBookings() {
        return bookingJdbc.query(
                "SELECT id, show_id, seat_ids, state, updated_at FROM booking",
                (rs, rowNum) -> new BookingRow(
                        rs.getString("id"),
                        rs.getString("show_id"),
                        parseSeatIds(rs.getString("seat_ids")),
                        rs.getString("state"),
                        rs.getTimestamp("updated_at").toInstant()
                )
        );
    }

    private List<SeatRow> loadSeats() {
        return inventoryJdbc.query(
                "SELECT show_id, seat_label, status, booking_id FROM seat",
                (rs, rowNum) -> new SeatRow(
                        rs.getString("show_id"),
                        rs.getString("seat_label"),
                        rs.getString("status"),
                        rs.getString("booking_id")
                )
        );
    }

    private Map<String, String> loadPaymentStatuses() {
        List<PaymentStatusRow> rows = paymentJdbc.query(
                "SELECT booking_id, status FROM payment",
                (rs, rowNum) -> new PaymentStatusRow(rs.getString("booking_id"), rs.getString("status"))
        );
        Map<String, String> result = new HashMap<>();
        for (PaymentStatusRow row : rows) {
            result.put(row.bookingId(), row.status());
        }
        return result;
    }

    private List<String> parseSeatIds(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<List<String>>() {
            });
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse seat_ids: " + json, e);
        }
    }

    private record BookingRow(String id, String showId, List<String> seatIds, String state, Instant updatedAt) {
    }

    private record SeatRow(String showId, String seatLabel, String status, String bookingId) {
    }

    private record PaymentStatusRow(String bookingId, String status) {
    }
}