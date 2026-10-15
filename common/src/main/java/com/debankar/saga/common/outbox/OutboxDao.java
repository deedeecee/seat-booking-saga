package com.debankar.saga.common.outbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Reads and writes the {@code outbox_message} table. {@link #insert} takes
 * part in whatever transaction is already open on the calling thread, so a
 * business write and its outbox row commit or roll back together.
 */
public class OutboxDao {

    private static final RowMapper<OutboxRecord> ROW_MAPPER = (rs, rowNum) -> new OutboxRecord(
            rs.getObject("id", UUID.class),
            rs.getString("aggregate_type"),
            rs.getString("aggregate_id"),
            rs.getString("topic"),
            rs.getString("message_key"),
            rs.getString("event_type"),
            rs.getString("payload"),
            rs.getTimestamp("created_at").toInstant()
    );

    private final JdbcTemplate jdbcTemplate;

    public OutboxDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(OutboxRecord record) {
        jdbcTemplate.update(
                """
                INSERT INTO outbox_message
                    (id, aggregate_type, aggregate_id, topic, message_key, event_type, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                record.id(),
                record.aggregateType(),
                record.aggregateId(),
                record.topic(),
                record.messageKey(),
                record.eventType(),
                record.payload(),
                Timestamp.from(record.createdAt())
        );
    }

    /**
     * Locks and returns up to {@code limit} unpublished rows, oldest first,
     * skipping rows already locked by another poller. Call this inside a
     * transaction so the lock is held until {@link #markPublished} runs.
     */
    public List<OutboxRecord> findUnpublished(int limit) {
        return jdbcTemplate.query(
                """
                SELECT id, aggregate_type, aggregate_id, topic, message_key, event_type, payload, created_at
                FROM outbox_message
                WHERE published_at IS NULL
                ORDER BY created_at
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """,
                ROW_MAPPER,
                limit
        );
    }

    public void markPublished(UUID id) {
        jdbcTemplate.update(
                "UPDATE outbox_message SET published_at = ? WHERE id = ?",
                Timestamp.from(Instant.now()),
                id
        );
    }

    public int countUnpublished() {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_message WHERE published_at IS NULL",
                Integer.class
        );
        return count == null ? 0 : count;
    }
}