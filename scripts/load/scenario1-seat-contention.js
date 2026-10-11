import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

// 500 seats, 5000 requests — exactly 10 competing attempts per seat. Every
// booking id is deterministic and unique: seat number + attempt number +
// this run's own id, so two runs of this script never collide, and every
// one of the 10 attempts on a given seat is a genuinely distinct customer,
// not a retry of the same idempotency key.
const TOTAL_SEATS = 500;
const ATTEMPTS_PER_SEAT = 10;
const TOTAL_REQUESTS = TOTAL_SEATS * ATTEMPTS_PER_SEAT;

const BOOKING_URL = __ENV.BOOKING_URL || 'http://localhost:8081';
const INVENTORY_URL = __ENV.INVENTORY_URL || 'http://localhost:8082';
const RUN_ID = __ENV.RUN_ID || String(Date.now());

export const options = {
  scenarios: {
    seat_contention: {
      executor: 'shared-iterations',
      vus: 50,
      iterations: TOTAL_REQUESTS,
      maxDuration: '5m',
    },
  },
};

// Runs once, before any VU starts iterating — the right place to seed the
// show every iteration below will book against.
export function setup() {
  const seatLabels = [];
  for (let i = 1; i <= TOTAL_SEATS; i++) {
    seatLabels.push(`S${i}`);
  }

  const res = http.post(
    `${INVENTORY_URL}/shows`,
    JSON.stringify({
      title: 'Load Test Show',
      venue: 'Load Test Arena',
      startsAt: '2026-12-01T19:00:00Z',
      seatLabels: seatLabels,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  if (res.status !== 201) {
    throw new Error(`Failed to seed show: ${res.status} ${res.body}`);
  }

  const showId = JSON.parse(res.body).showId;
  console.log(`Seeded show ${showId} with ${TOTAL_SEATS} seats. Run id: ${RUN_ID}`);
  return { showId };
}

export default function (data) {
  // iterationInTest is unique and zero-based across every VU — exactly
  // what deterministic seat assignment needs.
  const iteration = exec.scenario.iterationInTest;
  const seatNumber = (iteration % TOTAL_SEATS) + 1;
  const attemptNumber = Math.floor(iteration / TOTAL_SEATS);
  const seatLabel = `S${seatNumber}`;
  const bookingId = `load-scenario1-seat${seatNumber}-attempt${attemptNumber}-${RUN_ID}`;

  const res = http.post(
    `${BOOKING_URL}/bookings`,
    JSON.stringify({
      showId: data.showId,
      seatIds: [seatLabel],
      amountCents: 2500,
    }),
    {
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': bookingId,
      },
    }
  );

  // Every request gets a 202 regardless of outcome — the seat-level
  // accept-or-reject decision happens asynchronously, after this response
  // has already returned. That split is what the wait script checks next.
  check(res, { 'booking request accepted (202)': (r) => r.status === 202 });
}