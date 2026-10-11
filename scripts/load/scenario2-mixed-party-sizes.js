import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

// 200 seats. Every one of 197 possible starting positions gets four
// competing party sizes — 1, 2, 3, and 4 seats, all starting at that same
// position — so almost every seat is claimed by multiple overlapping
// requests of different sizes at once: the shape that stresses
// all-or-nothing, multi-seat locking, not just single-seat contention.
const TOTAL_SEATS = 200;
const PARTY_SIZES = [1, 2, 3, 4];
const MAX_PARTY_SIZE = 4;
const STARTING_POSITIONS = TOTAL_SEATS - MAX_PARTY_SIZE + 1; // 197: every size stays in bounds
const TOTAL_REQUESTS = STARTING_POSITIONS * PARTY_SIZES.length; // 788

const BOOKING_URL = __ENV.BOOKING_URL || 'http://localhost:8081';
const INVENTORY_URL = __ENV.INVENTORY_URL || 'http://localhost:8082';
const RUN_ID = __ENV.RUN_ID || String(Date.now());

export const options = {
  scenarios: {
    mixed_party_sizes: {
      executor: 'shared-iterations',
      vus: 50,
      iterations: TOTAL_REQUESTS,
      maxDuration: '5m',
    },
  },
};

export function setup() {
  const seatLabels = [];
  for (let i = 1; i <= TOTAL_SEATS; i++) {
    seatLabels.push(`S${i}`);
  }

  const res = http.post(
    `${INVENTORY_URL}/shows`,
    JSON.stringify({
      title: 'Load Test Show (mixed party sizes)',
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
  const iteration = exec.scenario.iterationInTest;
  const startingPositionIndex = Math.floor(iteration / PARTY_SIZES.length);
  const partySize = PARTY_SIZES[iteration % PARTY_SIZES.length];
  const startSeat = startingPositionIndex + 1;

  const seatLabels = [];
  for (let s = 0; s < partySize; s++) {
    seatLabels.push(`S${startSeat + s}`);
  }

  const bookingId = `load-scenario2-start${startSeat}-size${partySize}-${RUN_ID}`;

  const res = http.post(
    `${BOOKING_URL}/bookings`,
    JSON.stringify({
      showId: data.showId,
      seatIds: seatLabels,
      amountCents: partySize * 2500,
    }),
    {
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': bookingId,
      },
    }
  );

  check(res, { 'booking request accepted (202)': (r) => r.status === 202 });
}