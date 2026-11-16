// Circuit breaker demo: hammer /api/fail (500) and /api/slow (slow calls)
// so the gateway's Resilience4j breaker opens and /fallback returns 503.
// Watch the breaker state in Grafana / Prometheus while this runs.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8081';
const SLOW_MS = __ENV.SLOW_MS || '2000';

const fallback503 = new Counter('breaker_fallback_503');
const upstream500 = new Counter('upstream_500');
const limited = new Counter('rate_limited_429');

// Failures are the point of this test; don't count them as http_req_failed.
http.setResponseCallback(http.expectedStatuses(200, 429, 500, 503));

export const options = {
  scenarios: {
    fail: { executor: 'constant-arrival-rate', rate: 8, timeUnit: '1s', duration: '1m', preAllocatedVUs: 20, maxVUs: 100, exec: 'fail' },
    slow: { executor: 'constant-arrival-rate', rate: 4, timeUnit: '1s', duration: '1m', preAllocatedVUs: 20, maxVUs: 100, exec: 'slow' },
    // After the breaker should be open, check that healthy endpoints also get the fast fallback.
    probe: { executor: 'constant-arrival-rate', rate: 1, timeUnit: '1s', duration: '1m', preAllocatedVUs: 5, maxVUs: 20, exec: 'probe', startTime: '20s' },
  },
  thresholds: {
    // Expect the breaker to have opened at least once.
    breaker_fallback_503: ['count>0'],
  },
};

function record(res) {
  if (res.status === 503) fallback503.add(1);
  else if (res.status === 500) upstream500.add(1);
  else if (res.status === 429) limited.add(1);
}

export function fail() {
  const res = http.get(`${BASE}/api/fail`);
  record(res);
  check(res, { 'fail: 500, 429 or 503': (r) => [500, 429, 503].includes(r.status) });
}

export function slow() {
  const res = http.get(`${BASE}/api/slow?ms=${SLOW_MS}`);
  record(res);
  check(res, { 'slow: 200, 429 or 503': (r) => [200, 429, 503].includes(r.status) });
}

export function probe() {
  const res = http.get(`${BASE}/api/ping`);
  record(res);
  sleep(0);
}
