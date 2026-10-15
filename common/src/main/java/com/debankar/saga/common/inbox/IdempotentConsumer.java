package com.debankar.saga.common.inbox;

import com.debankar.saga.common.messaging.MessageEnvelope;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.transaction.support.TransactionTemplate;

public class IdempotentConsumer {

    private final InboxDao inboxDao;
    private final TransactionTemplate transactionTemplate;
    private final Counter duplicatesDroppedCounter;

    public IdempotentConsumer(InboxDao inboxDao, TransactionTemplate transactionTemplate, MeterRegistry meterRegistry) {
        this.inboxDao = inboxDao;
        this.transactionTemplate = transactionTemplate;
        this.duplicatesDroppedCounter = Counter.builder("saga.inbox.duplicates_dropped")
                .description("Messages skipped because they were already processed")
                .register(meterRegistry);
    }

    public boolean processOnce(MessageEnvelope envelope, Runnable effect) {
        return transactionTemplate.execute(status -> {
            if (inboxDao.alreadyProcessed(envelope.messageId())) {
                duplicatesDroppedCounter.increment();
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