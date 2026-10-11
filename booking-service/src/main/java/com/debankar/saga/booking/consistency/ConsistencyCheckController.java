package com.debankar.saga.booking.consistency;

import com.debankar.saga.common.consistency.ConsistencyChecker;
import com.debankar.saga.common.consistency.ConsistencyReport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;

/**
 * {@code 200} with an empty violation list when everything checks out,
 * {@code 409} with the violations listed when it doesn't — the non-2xx
 * status on failure matters specifically so a plain {@code curl -f} in a
 * shell script, the kind steps 8 and 9 will use, correctly treats a
 * dirty report as a failure without needing to parse the body itself.
 */
@RestController
public class ConsistencyCheckController {

    private final ConsistencyChecker consistencyChecker;
    private final Duration maxNonFinalAge;

    public ConsistencyCheckController(ConsistencyChecker consistencyChecker,
                                      @Value("${consistency.max-non-final-age-seconds}") long maxNonFinalAgeSeconds) {
        this.consistencyChecker = consistencyChecker;
        this.maxNonFinalAge = Duration.ofSeconds(maxNonFinalAgeSeconds);
    }

    @GetMapping("/internal/consistency-check")
    public ResponseEntity<ConsistencyReport> check() {
        ConsistencyReport report = consistencyChecker.check(maxNonFinalAge);
        HttpStatus status = report.isClean() ? HttpStatus.OK : HttpStatus.CONFLICT;
        return ResponseEntity.status(status).body(report);
    }
}