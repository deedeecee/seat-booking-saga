package com.debankar.saga.inventory.seat;

import java.util.List;

public record ReleaseResult(List<String> releasedSeatLabels) {
}