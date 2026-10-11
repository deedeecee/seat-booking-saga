package com.debankar.saga.common.inbox;

import com.debankar.saga.common.messaging.MessageEnvelope;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Runs a consumer's business effect at most once per inbound message.
 *
 * <p>{@link #processOnce} does the duplicate check, the effect, and the
 * inbox insert inside one transaction, taken from an injected
 * {@link TransactionTemplate} rather than an {@code @Transactional}
 * annotation on this class, for the same self-invocation reason documented
 * on {@code OutboxRelay}. Two callers can still race past the initial
 * duplicate check at the same time — Postgres's default isolation lets that
 * happen — so when that occurs, the {@code inbox_message} primary key lets
 * only one insert succeed, and the loser's transaction is deliberately
 * rolled back via {@code setRollbackOnly()} so its copy of the effect never
 * commits.
 */
public class IdempotentConsumer {

    private final InboxDao inboxDao;
    private final TransactionTemplate transactionTemplate;

    public IdempotentConsumer(InboxDao inboxDao, TransactionTemplate transactionTemplate) {
        this.inboxDao = inboxDao;
        this.transactionTemplate = transactionTemplate;
    }

    /**
     * Returns {@code true} if {@code effect} ran (and its inbox row
     * committed) on this call, {@code false} if the message had already
     * been claimed and this call was a no-op.
     */
    public boolean processOnce(MessageEnvelope envelope, Runnable effect) {
        return transactionTemplate.execute(status -> {
            if (inboxDao.alreadyProcessed(envelope.messageId())) {
                return false;
            }

            effect.run();

            boolean claimed = inboxDao.markProcessed(envelope.messageId(), envelope.eventType());
            if (!claimed) {
                status.setRollbackOnly();
                return false;
            }
            return true;
        });
    }
}