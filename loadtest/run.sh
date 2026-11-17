#!/usr/bin/env sh
# Runs a k6 script from a separate grafana/k6 container (outside the app containers).
# Usage: ./loadtest/run.sh [smoke|spike]   (default: smoke)
# Env:   BASE_URL (default http://host.docker.internal:8081), PEAK_RPS, SLOW_MS
set -eu

SCRIPT="${1:-smoke}"
DIR="$(cd "$(dirname "$0")" && pwd)"

[ -f "$DIR/$SCRIPT.js" ] || { echo "Unknown script '$SCRIPT'. Use smoke or spike." >&2; exit 1; }

exec docker run --rm -i \
  --add-host=host.docker.internal:host-gateway \
  -v "$DIR:/scripts:ro" \
  -e "BASE_URL=${BASE_URL:-http://host.docker.internal:8081}" \
  -e "PEAK_RPS=${PEAK_RPS:-500}" \
  -e "SLOW_MS=${SLOW_MS:-2000}" \
  grafana/k6 run "/scripts/$SCRIPT.js"
