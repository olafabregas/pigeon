#!/usr/bin/env sh
# Runs a k6 script from a separate grafana/k6 container (outside the app containers).
# Usage: ./loadtest/run.sh [smoke|spike]   (default: smoke)
# Env:   BASE_URL (default http://host.docker.internal:8081), PEAK_RPS, SLOW_MS
set -eu

SCRIPT="${1:-smoke}"
DIR="$(cd "$(dirname "$0")" && pwd)"

[ -f "$DIR/$SCRIPT.js" ] || { echo "Unknown script '$SCRIPT'. Use smoke or spike." >&2; exit 1; }

if [ -z "${BASE_URL:-}" ]; then
  CORE_PORT="$(docker compose port core 8081 2>/dev/null | tail -n 1)"
  if [ -n "$CORE_PORT" ]; then
    CORE_HOST="$(printf '%s' "$CORE_PORT" | sed 's/:.*//')"
    if [ "$CORE_HOST" = "0.0.0.0" ] || [ "$CORE_HOST" = "::" ]; then
      CORE_HOST="127.0.0.1"
    fi
    BASE_URL="http://${CORE_HOST}:$(printf '%s' "$CORE_PORT" | sed 's/.*://')"
  else
    echo "BASE_URL is required unless the Compose core service is running." >&2
    exit 1
  fi
fi

exec docker run --rm -i \
  --add-host=host.docker.internal:host-gateway \
  -v "$DIR:/scripts:ro" \
  -e "BASE_URL=${BASE_URL:-http://host.docker.internal:8081}" \
  -e "PEAK_RPS=${PEAK_RPS:-500}" \
  -e "SLOW_MS=${SLOW_MS:-2000}" \
  grafana/k6 run "/scripts/$SCRIPT.js"
