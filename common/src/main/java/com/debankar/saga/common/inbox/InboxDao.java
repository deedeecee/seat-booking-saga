package com.debankar.saga.common.inbox;

import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

/**
 * Reads and writes the {@code inbox_message} table: the consumer-side twin
 * of the outbox. A row here means "this message has been handled", keyed by
 * the same {@code messageId} the publishing service generated for its
 * outbox row, so a message redelivered by Kafka is recognized and skipped.
 */
public class InboxDao {

    private final JdbcTemplate jdbcTemplate;

    public InboxDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean alreadyProcessed(UUID messageId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM inbox_message WHERE message_id = ?",
                Integer.class,
                messageId
        );
        return count != null && count > 0;
    }

    /**
     * Claims {@code messageId} for this consumer. Returns {@code true} if
     * this call was the one that inserted the row, {@code false} if another
     * transaction already holds it — {@code ON CONFLICT DO NOTHING} means a
     * losing caller gets a clean "0 rows" result rather than a constraint-
     * violation exception to catch.
     */
    public boolean markProcessed(UUID messageId, String eventType) {
        int rows = jdbcTemplate.update(
                """
                INSERT INTO inbox_message (message_id, event_type, processed_at)
                VALUES (?, ?, ?)
                ON CONFLICT (message_id) DO NOTHING
                """,
                messageId,
                eventType,
                Timestamp.from(Instant.now())
        );
        return rows == 1;
    }
}