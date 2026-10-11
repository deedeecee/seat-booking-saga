package com.debankar.saga.inventory.seat;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * All seat state transitions for the saga: hold, confirm, and release.
 *
 * <p>Every method here runs with {@link TransactionDefinition#PROPAGATION_NESTED},
 * not the default {@code PROPAGATION_REQUIRED} — deliberately, not
 * interchangeably. {@code confirmSeats} can write a partial result (one
 * seat confirmed) before discovering the whole attempt must be rejected,
 * and needs to undo just that partial write. Called standalone, as in this
 * class's own tests, {@code NESTED} behaves exactly like {@code REQUIRED}
 * — there's no surrounding transaction to nest inside, so a fresh one
 * opens either way. Called from {@code InventoryCommandListener}, where a
 * whole command already runs inside {@code IdempotentConsumer}'s open
 * transaction, {@code REQUIRED} would have this class's own {@code
 * setRollbackOnly()} mark that OUTER transaction doomed too, so that when
 * {@code IdempotentConsumer} finishes its own, unrelated work and tries to
 * commit, Spring throws {@code UnexpectedRollbackException} — an ordinary,
 * expected saga outcome ends up looking like a processing failure and
 * heading toward retries and the dead-letter topic. {@code NESTED} uses a
 * real JDBC savepoint instead: rolling back to it undoes only this
 * method's own writes, leaving the surrounding transaction — and whatever
 * the caller does next in it, like publishing a rejection event —
 * completely unaffected.
 */
public class SeatHoldService {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final Duration holdDuration;

    public SeatHoldService(JdbcTemplate jdbcTemplate,
                           PlatformTransactionManager transactionManager,
                           Duration holdDuration) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
        this.holdDuration = holdDuration;
    }

    /**
     * Holds every requested seat for {@code bookingId}, or none of them.
     *
     * <p>Locks each requested seat with its own {@code SELECT ... FOR
     * UPDATE}, one statement per seat, issued in a fixed order — the seat
     * labels sorted alphabetically — rather than a single {@code SELECT
     * ... FOR UPDATE ORDER BY} query. Postgres doesn't guarantee {@code
     * FOR UPDATE} acquires locks in {@code ORDER BY}'s order, only that
     * the final result set comes back in that order, so it isn't a
     * reliable way to establish a lock order. Issuing the locks as
     * separate, sequential statements in a fixed order means every
     * concurrent request that overlaps on any seat acquires locks in the
     * same relative order, which rules out the circular wait that causes
     * a deadlock — at the cost of one extra round trip per seat instead
     * of one query for the whole request.
     */
    public HoldResult holdSeats(UUID showId, String bookingId, List<String> seatLabels) {
        List<String> sortedLabels = seatLabels.stream().sorted().toList();

        return transactionTemplate.execute(status -> {
            for (String label : sortedLabels) {
                List<String> rows = jdbcTemplate.query(
                        "SELECT status FROM seat WHERE show_id = ? AND seat_label = ? FOR UPDATE",
                        (rs, rowNum) -> rs.getString("status"),
                        showId, label
                );
                if (rows.isEmpty() || !"AVAILABLE".equals(rows.get(0))) {
                    status.setRollbackOnly();
                    return new HoldResult.Rejected(label);
                }
            }

            Instant expiresAt = Instant.now().plus(holdDuration);
            for (String label : sortedLabels) {
                jdbcTemplate.update(
                        """
                        UPDATE seat SET status = 'HELD', booking_id = ?, hold_expires_at = ?
                        WHERE show_id = ? AND seat_label = ?
                        """,
                        bookingId, Timestamp.from(expiresAt), showId, label
                );
            }
            return new HoldResult.Held(sortedLabels);
        });
    }

    /**
     * Confirms every requested seat for {@code bookingId}, or none of them.
     * A seat only confirms if it's still {@code HELD} by this exact booking
     * at the moment this runs — if the sweeper reclaimed it first, that
     * seat's per-row update simply won't match, the overall count falls
     * short, and the whole attempt rolls back to the savepoint this
     * method's own transaction opened.
     */
    public ConfirmResult confirmSeats(UUID showId, String bookingId, List<String> seatLabels) {
        return transactionTemplate.execute(status -> {
            int confirmedCount = 0;
            for (String label : seatLabels) {
                int rows = jdbcTemplate.update(
                        """
                        UPDATE seat SET status = 'BOOKED', hold_expires_at = NULL
                        WHERE show_id = ? AND seat_label = ? AND booking_id = ? AND status = 'HELD'
                        """,
                        showId, label, bookingId
                );
                confirmedCount += rows;
            }
            if (confirmedCount != seatLabels.size()) {
                status.setRollbackOnly();
                return new ConfirmResult.Rejected(seatLabels);
            }
            return new ConfirmResult.Confirmed(seatLabels);
        });
    }

    /**
     * Releases whichever requested seats are still held by {@code
     * bookingId}, best-effort. Unlike {@code confirmSeats}, this never
     * rejects: a seat that's already available — because it was released
     * by an earlier, redelivered copy of this same command, or reclaimed
     * by the sweeper — is simply skipped, not treated as an error. That
     * makes this method naturally safe to call twice with the same
     * arguments, on top of whatever protection {@code IdempotentConsumer}
     * adds at the listener level.
     */
    public ReleaseResult releaseSeats(UUID showId, String bookingId, List<String> seatLabels) {
        return transactionTemplate.execute(status -> {
            List<String> released = new ArrayList<>();
            for (String label : seatLabels) {
                int rows = jdbcTemplate.update(
                        """
                        UPDATE seat SET status = 'AVAILABLE', booking_id = NULL, hold_expires_at = NULL
                        WHERE show_id = ? AND seat_label = ? AND booking_id = ? AND status = 'HELD'
                        """,
                        showId, label, bookingId
                );
                if (rows > 0) {
                    released.add(label);
                }
            }
            return new ReleaseResult(released);
        });
    }
}