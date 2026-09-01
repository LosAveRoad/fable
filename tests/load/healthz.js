import http from 'k6/http';
import { check } from 'k6';

const base = (__ENV.BASE_URL || 'https://api.fableim.lol').replace(/\/$/, '');
const rate = Number(__ENV.TARGET_RPS || 100);

export const options = {
  scenarios: { health: { executor: 'constant-arrival-rate', rate, timeUnit: '1s', duration: __ENV.DURATION || '30s', preAllocatedVUs: Math.max(20, Math.ceil(rate / 2)), maxVUs: Math.max(100, rate * 2) } },
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<500', 'p(99)<1000'] },
};

export default function () {
  const response = http.get(`${base}/healthz`, { tags: { endpoint: 'healthz' } });
  check(response, { 'healthz is 200': (r) => r.status === 200 });
}
