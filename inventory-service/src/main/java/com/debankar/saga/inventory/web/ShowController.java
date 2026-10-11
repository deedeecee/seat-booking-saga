package com.debankar.saga.inventory.web;

import com.debankar.saga.inventory.seat.SeatDao;
import com.debankar.saga.inventory.seat.Show;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/shows")
public class ShowController {

    private final SeatDao seatDao;

    public ShowController(SeatDao seatDao) {
        this.seatDao = seatDao;
    }

    @PostMapping
    public ResponseEntity<SeedShowResponse> seedShow(@Valid @RequestBody SeedShowRequest request) {
        UUID showId = UUID.randomUUID();
        seatDao.insertShow(new Show(showId, request.title(), request.venue(), request.startsAt()));
        seatDao.insertSeats(showId, request.seatLabels());
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new SeedShowResponse(showId, request.seatLabels().size()));
    }

    @GetMapping("/{showId}/seats")
    public SeatMapResponse seatMap(@PathVariable UUID showId) {
        List<SeatView> seats = seatDao.findByShow(showId).stream()
                .map(seat -> new SeatView(
                        seat.seatLabel(), seat.status().name(), seat.bookingId(), seat.holdExpiresAt()))
                .toList();
        return new SeatMapResponse(showId, seats);
    }

    public record SeedShowRequest(
            @NotBlank String title,
            @NotBlank String venue,
            @NotNull Instant startsAt,
            @NotEmpty List<@NotBlank String> seatLabels
    ) {
    }

    public record SeedShowResponse(UUID showId, int seatCount) {
    }

    public record SeatView(String seatLabel, String status, String bookingId, Instant holdExpiresAt) {
    }

    public record SeatMapResponse(UUID showId, List<SeatView> seats) {
    }
}