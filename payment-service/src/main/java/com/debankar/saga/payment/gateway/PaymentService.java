package com.debankar.saga.payment.gateway;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;

/**
 * The mock gateway's own decision logic, and the transaction boundary
 * around each attempt. Uses {@link TransactionDefinition#PROPAGATION_NESTED}
 * for the same reason {@code SeatHoldService} does — see that class's own
 * Javadoc — even though nothing here currently does a multi-statement,
 * partial-write-then-rollback sequence the way {@code confirmSeats} does.
 * This class is composed inside {@code IdempotentConsumer}'s transaction
 * exactly the same way, so it inherits the same risk the moment any future
 * change here adds a second write to an authorize or void attempt.
 *
 * <p>Deterministic by booking-id prefix, so the load and chaos tests
 * (steps 8 and 9) can trigger specific outcomes on demand, without any
 * randomness that would make a run hard to reproduce: a booking id
 * starting with {@code "decline-"} always declines; one starting with
 * {@code "slow-"} authorizes normally but only after an artificial delay,
 * injected before the transaction opens rather than inside it, so a slow
 * payment doesn't also hold a database connection and row locks for the
 * whole delay.
 */
public class PaymentService {

    private static final String DECLINE_PREFIX = "decline-";
    private static final String SLOW_PREFIX = "slow-";

    private final PaymentDao paymentDao;
    private final TransactionTemplate transactionTemplate;
    private final Duration slowPaymentDelay;

    public PaymentService(PaymentDao paymentDao,
                          PlatformTransactionManager transactionManager,
                          Duration slowPaymentDelay) {
        this.paymentDao = paymentDao;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
        this.slowPaymentDelay = slowPaymentDelay;
    }

    public AuthorizeResult authorize(String bookingId, long amountCents) {
        if (bookingId.startsWith(SLOW_PREFIX)) {
            sleepUninterruptibly(slowPaymentDelay);
        }
        PaymentStatus decidedStatus = bookingId.startsWith(DECLINE_PREFIX)
                ? PaymentStatus.DECLINED : PaymentStatus.AUTHORIZED;

        return transactionTemplate.execute(status -> {
            Payment payment = paymentDao.insertIfAbsent(bookingId, decidedStatus, amountCents);
            return payment.status() == PaymentStatus.AUTHORIZED
                    ? new AuthorizeResult.Authorized(payment.amountCents())
                    : new AuthorizeResult.Declined("booking id starts with \"" + DECLINE_PREFIX + "\"");
        });
    }

    public void voidPayment(String bookingId) {
        transactionTemplate.executeWithoutResult(status -> paymentDao.voidPayment(bookingId));
    }

    private static void sleepUninterruptibly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while injecting slow-payment delay", e);
        }
    }
}