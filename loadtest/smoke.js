// Smoke test: low load, verifies the stack works end to end.
// Run via loadtest/run.ps1 or run.sh (k6 runs in its own container).
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8081';

export const options = {
  vus: 2,
  duration: '30s',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'checks{check:no_5xx}': ['rate>0.99'],
    http_req_duration: ['p(95)<1000'],
  },
};

http.setResponseCallback(http.expectedStatuses(200, 201));

export default function () {
  const payload = JSON.stringify({ customerId: `c${__VU}`, amount: 10.5 });
  const params = { headers: { 'Content-Type': 'application/json' } };

  const order = http.post(`${BASE}/api/orders`, payload, params);
  check(order, {
    'order 201': (r) => r.status === 201,
    'order 201 has id': (r) => r.status !== 201 || !!r.json('id'),
    no_5xx: (r) => r.status < 500,
  });

  const ping = http.get(`${BASE}/api/ping`);
  check(ping, {
    'ping 200': (r) => r.status === 200,
    no_5xx: (r) => r.status < 500,
  });

  sleep(0.5);
}
