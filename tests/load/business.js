import http from 'k6/http';
import { check, sleep } from 'k6';

const base = (__ENV.BASE_URL || 'https://api.fableim.lol').replace(/\/$/, '');
const token = __ENV.AUTH_TOKEN || '';
const groupID = __ENV.GROUP_ID || '';
export const options = {
  scenarios: { business: { executor: 'constant-arrival-rate', rate: Number(__ENV.TARGET_RPS || 20), timeUnit: '1s', duration: __ENV.DURATION || '30s', preAllocatedVUs: 20, maxVUs: 200 } },
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<1000', 'p(99)<2000'] },
};
const params = { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, tags: { endpoint: 'business' } };
export default function () {
  if (!token) throw new Error('AUTH_TOKEN is required');
  const list = http.post(`${base}/group/list/joined`, null, params);
  check(list, { 'joined groups succeeds': (r) => r.status === 200 });
  if (groupID) {
    const messages = http.post(`${base}/group/message/list`, JSON.stringify({ group_id: groupID }), params);
    check(messages, { 'group messages succeeds': (r) => r.status === 200 });
  }
  sleep(0.2);
}
