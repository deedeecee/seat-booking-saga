package com.debankar.saga.booking.web;

import com.debankar.saga.booking.saga.Booking;
import com.debankar.saga.booking.saga.BookingDao;
import com.debankar.saga.booking.saga.BookingOrchestrator;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/bookings")
@Validated
public class BookingController {

    private final BookingOrchestrator bookingOrchestrator;
    private final BookingDao bookingDao;

    public BookingController(BookingOrchestrator bookingOrchestrator, BookingDao bookingDao) {
        this.bookingOrchestrator = bookingOrchestrator;
        this.bookingDao = bookingDao;
    }

    @PostMapping
    public ResponseEntity<BookingResponse> createBooking(
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 100) String idempotencyKey,
            @Valid @RequestBody CreateBookingRequest request) {
        Booking booking = bookingOrchestrator.createIfAbsent(
                idempotencyKey, request.showId(), request.seatIds(), request.amountCents());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(toResponse(booking));
    }

    @GetMapping("/{id}")
    public ResponseEntity<BookingResponse> getBooking(@PathVariable String id) {
        return bookingDao.findById(id)
                .map(booking -> ResponseEntity.ok(toResponse(booking)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private BookingResponse toResponse(Booking booking) {
        return new BookingResponse(
                booking.id(), booking.showId(), booking.seatIds(), booking.amountCents(), booking.state().name());
    }

    public record CreateBookingRequest(
            @NotNull UUID showId,
            @NotEmpty List<@NotBlank String> seatIds,
            @Positive long amountCents
    ) {
    }

    public record BookingResponse(String bookingId, UUID showId, List<String> seatIds, long amountCents, String state) {
    }
}