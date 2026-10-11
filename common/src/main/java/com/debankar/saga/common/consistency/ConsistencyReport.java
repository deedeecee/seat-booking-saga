package com.debankar.saga.common.consistency;

import java.util.List;

public record ConsistencyReport(List<Violation> violations) {

    public boolean isClean() {
        return violations.isEmpty();
    }
}