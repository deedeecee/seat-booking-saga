#!/usr/bin/env bash
# Polls booking_db until no booking whose id matches $1 (a SQL LIKE
# pattern) is still in a non-final state, then prints the final state
# breakdown. Usage: wait-for-bookings.sh 'load-scenario1-%-1733500000' [max_wait_seconds]
set -uo pipefail

PATTERN="$1"
MAX_WAIT="${2:-300}"
WAITED=0

echo "Waiting for all bookings matching '$PATTERN' to reach a final state..."
while true; do
  STUCK=$(docker compose exec -T postgres psql -U saga -d booking_db -t -A -c \
    "select count(*) from booking where id like '$PATTERN' and state not in ('CONFIRMED','CANCELLED','REJECTED');" \
    2>/dev/null | tr -d '[:space:]')
  if [ "$STUCK" = "0" ]; then
    echo "All bookings reached a final state after ${WAITED}s."
    break
  fi
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    echo "Timed out after ${MAX_WAIT}s with $STUCK booking(s) still non-final."
    break
  fi
  sleep 2
  WAITED=$((WAITED + 2))
done

echo
echo "Final state breakdown:"
docker compose exec -T postgres psql -U saga -d booking_db -c \
  "select state, count(*) from booking where id like '$PATTERN' group by state order by state;"