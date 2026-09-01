import ws from 'k6/ws';
import { check } from 'k6';

const base = (__ENV.WS_BASE || 'wss://api.fableim.lol').replace(/\/$/, '');
export const options = { scenarios: { sockets: { executor: 'constant-vus', vus: Number(__ENV.VUS || 25), duration: __ENV.DURATION || '30s' } }, thresholds: { ws_connecting: ['p(95)<1000'], checks: ['rate>0.99'] } };
export default function () {
  const token = __ENV.WS_TOKEN;
  if (!token) throw new Error('WS_TOKEN is required');
  const response = ws.connect(`${base}/wss?token=${encodeURIComponent(token)}`, {}, (socket) => {
    socket.on('open', () => socket.setTimeout(() => socket.close(), 5000));
  });
  check(response, { 'websocket upgrade succeeds': (r) => r && r.status === 101 });
}
