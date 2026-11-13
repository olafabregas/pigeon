// Spike test: ramp to a high request rate against the gateway.
// 429 is EXPECTED (rate limiter doing its job). 5xx is not.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const PEAK_RPS = parseInt(__ENV.PEAK_RPS || '500', 10);

const created = new Counter('orders_created_201');
const limited = new Counter('rate_limited_429');
const serverErrors = new Counter('server_errors_5xx');

http.setResponseCallback(http.expectedStatuses(200, 201, 429));

export const options = {
  scenarios: {
    spike: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 1000,
      stages: [
        { target: 50, duration: '30s' },
        { target: PEAK_RPS, duration: '30s' },
        { target: PEAK_RPS, duration: '1m' },
        { target: 20, duration: '20s' },
        { target: 0, duration: '10s' },
      ],
    },
  },
  thresholds: {
    server_errors_5xx: ['count==0'],
    'checks{check:no_5xx}': ['rate>0.999'],
    http_req_duration: ['p(95)<2000'],
  },
};

export default function () {
  let res;
  if (Math.random() < 0.7) {
    res = http.post(
      `${BASE}/api/orders`,
      JSON.stringify({ customerId: `c${__VU}`, amount: Math.round(Math.random() * 10000) / 100 }),
      { headers: { 'Content-Type': 'application/json' } },
    );
    check(res, { 'order 201 or 429': (r) => r.status === 201 || r.status === 429 });
  } else {
    res = http.get(`${BASE}/api/ping`);
    check(res, { 'ping 200 or 429': (r) => r.status === 200 || r.status === 429 });
  }

  check(res, { no_5xx: (r) => r.status < 500 });
  if (res.status === 201) created.add(1);
  else if (res.status === 429) limited.add(1);
  else if (res.status >= 500) serverErrors.add(1);
}
