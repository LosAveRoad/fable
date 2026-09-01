import http from 'k6/http';
import { check, sleep } from 'k6';

const base = (__ENV.BASE_URL || 'https://api.fableim.lol').replace(/\/$/, '');
export const options = {
  stages: [{ duration: '15s', target: 10 }, { duration: '30s', target: 50 }, { duration: '30s', target: 100 }, { duration: '15s', target: 0 }],
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<500', 'p(99)<1500'] },
};
export default function () {
  const response = http.get(`${base}/healthz`, { tags: { endpoint: 'baseline' } });
  check(response, { 'baseline responds': (r) => r.status === 200 });
  sleep(0.1);
}
