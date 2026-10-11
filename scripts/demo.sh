#!/usr/bin/env bash
#
# End-to-end demo of the seat booking saga. Seeds one fresh show, then runs
# all six roadmap scenarios against the live docker-compose stack, printing
# PASS/FAIL for each. Requires the stack already up (docker compose up -d)
# and curl, jq, and docker on PATH.
set -uo pipefail

cd "$(dirname "$0")/.."

BOOKING_URL="${BOOKING_URL:-http://localhost:8081}"
INVENTORY_URL="${INVENTORY_URL:-http://localhost:8082}"
RUN_ID="$(date +%s)"

PASS_COUNT=0
FAIL_COUNT=0

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required tool: $1" >&2; exit 1; }
}
require curl
require jq
require docker

pass() { echo "  PASS: $1"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { echo "  FAIL: $1"; FAIL_COUNT=$((FAIL_COUNT + 1)); }

create_booking() {
  local idempotency_key="$1" show_id="$2" seat_id="$3" amount="$4"
  local body
  body=$(printf '{"showId":"%s","seatIds":["%s"],"amountCents":%s}' "$show_id" "$seat_id" "$amount")
  curl -s -X POST "$BOOKING_URL/bookings" -H "Content-Type: application/json" -H "Idempotency-Key: $idempotency_key" -d "$body"
}

get_booking_state() {
  curl -s "$BOOKING_URL/bookings/$1" | jq -r '.state // "NOT_FOUND"'
}

get_seat_status() {
  curl -s "$INVENTORY_URL/shows/$1/seats" | jq -r --arg seat "$2" '.seats[] | select(.seatLabel == $seat) | .status'
}

get_payment_status() {
  docker compose exec -T postgres psql -U saga -d payment_db -t -A -c "select status from payment where booking_id = '$1';" 2>/dev/null | tr -d '[:space:]'
}

count_outbox_events() {
  local booking_id="$1" event_type="$2"
  docker compose exec -T postgres psql -U saga -d booking_db -t -A -c "select count(*) from outbox_message where aggregate_id = '$booking_id' and event_type = '$event_type';" 2>/dev/null | tr -d '[:space:]'
}

wait_for_booking_state() {
  local booking_id="$1"; shift
  local wanted=("$@")
  local tries=0 max_tries=30 state
  while [ "$tries" -lt "$max_tries" ]; do
    state="$(get_booking_state "$booking_id")"
    for w in "${wanted[@]}"; do
      [ "$state" = "$w" ] && { echo "$state"; return 0; }
    done
    sleep 0.5
    tries=$((tries + 1))
  done
  echo "$state"
}

echo "Checking the stack is up..."
if ! curl -sf "$BOOKING_URL/actuator/health" | jq -e '.status == "UP"' >/dev/null 2>&1; then
  echo "booking-service isn't responding as healthy at $BOOKING_URL." >&2
  echo "Run 'docker compose up -d', wait about 20 seconds, then try again." >&2
  exit 1
fi

echo "=== Seat Booking Saga: end-to-end demo (run $RUN_ID) ==="
echo
echo "Seeding a fresh show..."
SHOW_ID="$(curl -s -X POST "$INVENTORY_URL/shows" -H "Content-Type: application/json" -d '{"title":"Demo Show","venue":"Demo Arena","startsAt":"2026-12-01T19:00:00Z","seatLabels":["A1","A2","A3","B1"]}' | jq -r '.showId')"
echo "Show: $SHOW_ID"
echo

echo "[1/6] Happy path"
BOOKING_1="demo-happy-path-$RUN_ID"
create_booking "$BOOKING_1" "$SHOW_ID" "A1" 3000 >/dev/null
STATE_1="$(wait_for_booking_state "$BOOKING_1" CONFIRMED CANCELLED REJECTED)"
SEAT_1="$(get_seat_status "$SHOW_ID" A1)"
if [ "$STATE_1" = "CONFIRMED" ] && [ "$SEAT_1" = "BOOKED" ]; then
  pass "booking reached CONFIRMED, seat A1 is BOOKED"
else
  fail "expected CONFIRMED+BOOKED, got booking=$STATE_1 seat=$SEAT_1"
fi
echo

echo "[2/6] Duplicate request (same key, sent again after completion)"
DUP_RESPONSE="$(create_booking "$BOOKING_1" "$SHOW_ID" "A1" 3000)"
DUP_ID="$(echo "$DUP_RESPONSE" | jq -r '.bookingId')"
DUP_STATE="$(echo "$DUP_RESPONSE" | jq -r '.state')"
HOLD_COUNT="$(count_outbox_events "$BOOKING_1" HoldSeats)"
if [ "$DUP_ID" = "$BOOKING_1" ] && [ "$DUP_STATE" = "CONFIRMED" ] && [ "$HOLD_COUNT" = "1" ]; then
  pass "repeated POST returned the same booking at CONFIRMED; only one HoldSeats was ever recorded"
else
  fail "expected id=$BOOKING_1 state=CONFIRMED holdCount=1, got id=$DUP_ID state=$DUP_STATE holdCount=$HOLD_COUNT"
fi
echo

echo "[3/6] Seat conflict"
BOOKING_3A="demo-seat-conflict-a-$RUN_ID"
BOOKING_3B="demo-seat-conflict-b-$RUN_ID"
create_booking "$BOOKING_3A" "$SHOW_ID" "A2" 3000 >/dev/null
create_booking "$BOOKING_3B" "$SHOW_ID" "A2" 3000 >/dev/null
STATE_3A="$(wait_for_booking_state "$BOOKING_3A" CONFIRMED REJECTED)"
STATE_3B="$(wait_for_booking_state "$BOOKING_3B" CONFIRMED REJECTED)"
ONE_REJECTED=0
if [ "$STATE_3A" = "REJECTED" ] && [ "$STATE_3B" != "REJECTED" ]; then ONE_REJECTED=1; fi
if [ "$STATE_3B" = "REJECTED" ] && [ "$STATE_3A" != "REJECTED" ]; then ONE_REJECTED=1; fi
if [ "$ONE_REJECTED" = "1" ]; then
  pass "exactly one of the two competing bookings was rejected"
else
  fail "expected exactly one REJECTED, got a=$STATE_3A b=$STATE_3B"
fi
echo

echo "[4/6] Payment declined"
BOOKING_4="decline-demo-$RUN_ID"
create_booking "$BOOKING_4" "$SHOW_ID" "A3" 3000 >/dev/null
STATE_4="$(wait_for_booking_state "$BOOKING_4" CONFIRMED CANCELLED)"
PAYMENT_4="$(get_payment_status "$BOOKING_4")"
SEAT_4="$(get_seat_status "$SHOW_ID" A3)"
if [ "$STATE_4" = "CANCELLED" ] && [ "$PAYMENT_4" = "DECLINED" ] && [ "$SEAT_4" = "AVAILABLE" ]; then
  pass "booking CANCELLED, payment DECLINED, seat A3 released"
else
  fail "expected CANCELLED+DECLINED+AVAILABLE, got booking=$STATE_4 payment=$PAYMENT_4 seat=$SEAT_4"
fi
echo

echo "[5/6] Hold expiry (payment-service stopped so authorization never responds in time)"
BOOKING_5="demo-hold-expiry-$RUN_ID"
docker compose stop payment-service >/dev/null
create_booking "$BOOKING_5" "$SHOW_ID" "B1" 3000 >/dev/null
echo "  waiting ~20s for the 15s hold to expire and the sweeper to react..."
sleep 20
STATE_5="$(get_booking_state "$BOOKING_5")"
SEAT_5="$(get_seat_status "$SHOW_ID" B1)"
if [ "$STATE_5" = "CANCELLED" ] && [ "$SEAT_5" = "AVAILABLE" ]; then
  pass "hold expired, booking CANCELLED, seat B1 released"
else
  fail "expected CANCELLED+AVAILABLE, got booking=$STATE_5 seat=$SEAT_5"
fi
echo

echo "[6/6] Late payment (payment-service restarts and authorizes after the fact)"
docker compose start payment-service >/dev/null
echo "  waiting ~10s for payment-service to start and drain its backlog..."
sleep 10
PAYMENT_6="$(get_payment_status "$BOOKING_5")"
STATE_6="$(get_booking_state "$BOOKING_5")"
if [ "$PAYMENT_6" = "VOIDED" ] && [ "$STATE_6" = "CANCELLED" ]; then
  pass "late authorization correctly VOIDED, booking still CANCELLED, not resurrected"
else
  fail "expected payment=VOIDED booking=CANCELLED, got payment=$PAYMENT_6 booking=$STATE_6"
fi
echo

echo "=== Results: $PASS_COUNT passed, $FAIL_COUNT failed ==="
[ "$FAIL_COUNT" -eq 0 ]