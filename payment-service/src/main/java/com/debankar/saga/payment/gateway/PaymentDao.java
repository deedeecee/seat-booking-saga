package com.debankar.saga.payment.gateway;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.Optional;

/**
 * Reads and writes the {@code payment} table. {@code insertIfAbsent} is the
 * only write that matters here: a payment is authorized-or-declined exactly
 * once per booking (the row's primary key is the booking id itself), so a
 * second attempt for the same booking is a no-op insert followed by a read
 * of whatever the FIRST attempt actually decided — ground truth, not a
 * fresh decision. {@code voidPayment} is the one exception, a plain
 * guarded update.
 */
public class PaymentDao {

    private static final RowMapper<Payment> PAYMENT_ROW_MAPPER = (rs, rowNum) -> new Payment(
            rs.getString("booking_id"),
            PaymentStatus.valueOf(rs.getString("status")),
            rs.getLong("amount_cents"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant()
    );

    private final JdbcTemplate jdbcTemplate;

    public PaymentDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Payment insertIfAbsent(String bookingId, PaymentStatus status, long amountCents) {
        jdbcTemplate.update(
                """
                INSERT INTO payment (booking_id, status, amount_cents)
                VALUES (?, ?, ?)
                ON CONFLICT (booking_id) DO NOTHING
                """,
                bookingId, status.name(), amountCents
        );
        return findByBookingId(bookingId)
                .orElseThrow(() -> new IllegalStateException(
                        "Payment row missing immediately after insert: " + bookingId));
    }

    public Optional<Payment> findByBookingId(String bookingId) {
        return jdbcTemplate.query(
                "SELECT booking_id, status, amount_cents, created_at, updated_at FROM payment WHERE booking_id = ?",
                PAYMENT_ROW_MAPPER,
                bookingId
        ).stream().findFirst();
    }

    /** Voids an authorized payment. A no-op if it's already voided or was never authorized. */
    public void voidPayment(String bookingId) {
        jdbcTemplate.update(
                "UPDATE payment SET status = 'VOIDED', updated_at = now() WHERE booking_id = ? AND status = 'AUTHORIZED'",
                bookingId
        );
    }
}