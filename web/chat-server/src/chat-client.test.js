import test from 'node:test';
import assert from 'node:assert/strict';
import { ChatClient } from './chat-client.js';

const a = { id: 'direct:S1', receive_id: 'U2', receive_type: 'user' };
const b = { id: 'group:G1', receive_id: 'G1', receive_type: 'group' };
const message = (id, position, conversation = a) => ({ message_id: id, position,
  conversation_id: conversation.id, session_id: conversation.id.split(':')[1],
  send_id: 'U1', receive_id: conversation.receive_id, receive_type: conversation.receive_type,
  content: 'hello', type: 'text', origin: 'user', created_at: '2026-10-31T00:00:00Z' });
const page = (messages = [], next_cursor = '', has_more = false, conversation = a) => ({
  conversation_id: conversation.id, messages, next_cursor, has_more,
});
const flush = async () => { for (let i = 0; i < 12; i++) await Promise.resolve(); };
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
function harness() {
  let now = 0, counter = 0;
  const timers = new Map(), sockets = [], requests = [], changes = [], errors = [];
  const clock = {
    setTimeout(callback, delay) { const id = ++counter; timers.set(id, { at: now + delay, callback }); return id; },
    clearTimeout(id) { timers.delete(id); },
    async tick(ms) {
      const end = now + ms;
      for (;;) {
        const next = [...timers.entries()].filter(([, t]) => t.at <= end).sort((x, y) => x[1].at - y[1].at)[0];
        if (!next) break;
        now = next[1].at;
        timers.delete(next[0]);
        next[1].callback();
        await flush();
      }
      now = end;
    },
  };
  const client = new ChatClient({ userID: 'U1', token: 'private-token', wsBase: 'ws://localhost:18080',
    clock, uuid: () => 'stable-request',
    createSocket(url) {
      const socket = { url, readyState: 0, sent: [],
        open() { this.readyState = 1; this.onopen?.(); },
        send(raw) { this.sent.push(raw); },
        message(frame) { this.onmessage?.({ data: typeof frame === 'string' ? frame : JSON.stringify(frame) }); },
        close() { this.readyState = 3; this.onclose?.({ code: 1006 }); },
      };
      sockets.push(socket);
      return socket;
    },
    fetchPage(conversation, cursor, signal) {
      const response = deferred();
      requests.push({ conversation, cursor, signal, ...response });
      return response.promise;
    },
    onChange: snapshot => changes.push(snapshot), onError: error => errors.push(error),
  });
  return { client, clock, sockets, requests, timers, changes, errors };
}

test('subscribes with explicit v2 before paging and merges live frames across page boundaries', async () => {
  const h = harness();
  h.client.start(); h.client.select(a);
  assert.equal(h.requests.length, 0);
  assert.equal(h.sockets.length, 1);
  const url = new URL(h.sockets[0].url);
  assert.equal(url.pathname, '/wss');
  assert.equal(url.searchParams.get('protocol'), 'v2');
  assert.equal(url.searchParams.get('token'), 'private-token');
  h.sockets[0].open();
  assert.equal(h.requests[0].cursor, '');
  h.sockets[0].message({ event: 'message', message: message('M3', '9007199254740993') });
  h.requests[0].resolve(page([message('M1', '1')], 'cursor-1', true));
  await flush();
  assert.equal(h.requests[1].cursor, 'cursor-1');
  h.sockets[0].message({ event: 'message', message: message('M2', '9007199254740992') });
  h.requests[1].resolve(page([message('M2', '9007199254740992')], 'cursor-2'));
  await flush();
  assert.deepEqual(h.changes.at(-1).messages.map(m => m.message_id), ['M1', 'M2', 'M3']);
  assert.equal(h.client.state.chats.get(a.id).cursor, 'cursor-2');
  h.client.dispose();
});

test('late sync replies cannot cross a conversation switch or logout', async () => {
  const h = harness();
  h.client.start(); h.client.select(a); h.sockets[0].open();
  const old = h.requests[0];
  h.client.select(b);
  assert.equal(old.signal.aborted, true);
  old.resolve(page([message('M-old', '1')], 'stale-cursor'));
  await flush();
  assert.equal(h.changes.at(-1).conversationID, b.id);
  assert.deepEqual(h.changes.at(-1).messages, []);
  assert.equal(h.client.state.chats.get(a.id).cursor, '');
  const count = h.changes.length;
  h.client.dispose();
  h.requests.at(-1).resolve(page([message('M-late', '2', b)], 'late', false, b));
  h.sockets[0].message({ event: 'message', message: message('M-ghost', '3', b) });
  await flush();
  await h.clock.tick(90000);
  assert.equal(h.changes.length, count);
  assert.equal(h.timers.size, 0);
});

test('reconnect resumes only from merged sync cursor and keeps same pending request for retry', async () => {
  const h = harness();
  h.client.start(); h.client.select(a); h.sockets[0].open();
  h.requests[0].resolve(page([message('M1', '1')], 'cursor-1'));
  await flush();
  h.client.send('hello');
  const sent = h.sockets[0].sent[0];
  assert.deepEqual(JSON.parse(sent), { event: 'send_message', client_message_id: 'stable-request',
    receive_id: 'U2', receive_type: 'user', content: 'hello' });
  h.sockets[0].message({ event: 'message', message: message('M9', '9') });
  h.sockets[0].close();
  assert.equal(h.client.state.snapshot(a.id).at(-1).status, 'uncertain');
  await h.clock.tick(1000);
  assert.equal(h.sockets.length, 2);
  assert.equal(h.sockets[1].sent.length, 0);
  h.sockets[1].open();
  assert.equal(h.requests.at(-1).cursor, 'cursor-1');
  h.client.retry('stable-request');
  assert.equal(h.sockets[1].sent[0], sent);
  h.client.dispose();
});

test('paging failure keeps buffered messages and retries from the last completed page', async () => {
  const h = harness();
  h.client.start(); h.client.select(a); h.sockets[0].open();
  h.requests[0].resolve(page([message('M1', '1')], 'cursor-1', true));
  await flush();
  h.sockets[0].message({ event: 'message', message: message('M3', '3') });
  h.requests[1].reject(Error('offline'));
  await flush();
  assert.deepEqual(h.changes.at(-1).messages.map(m => m.message_id), ['M1', 'M3']);
  await h.clock.tick(1000);
  assert.equal(h.requests[2].cursor, 'cursor-1');
  h.requests[2].resolve(page([message('M2', '2')], 'cursor-2'));
  await flush();
  assert.deepEqual(h.changes.at(-1).messages.map(m => m.message_id), ['M1', 'M2', 'M3']);
  h.client.dispose();
});

test('periodic repair follows the current conversation without opening another socket', async () => {
  const h = harness();
  h.client.start(); h.client.select(a); h.sockets[0].open();
  h.requests[0].resolve(page()); await flush();
  h.client.select(b);
  h.requests[1].resolve(page([], '', false, b)); await flush();
  await h.clock.tick(30000);
  assert.equal(h.requests.length, 3);
  assert.equal(h.requests[2].conversation, b.id);
  assert.equal(h.sockets.length, 1);
  h.client.dispose();
});

test('a send timeout becomes uncertain, triggers repair, and never silently resends', async () => {
  const h = harness();
  h.client.start(); h.client.select(a); h.sockets[0].open();
  h.requests[0].resolve(page()); await flush();
  h.client.send('hello');
  await h.clock.tick(12000);
  assert.equal(h.client.state.snapshot(a.id)[0].status, 'uncertain');
  assert.equal(h.requests.length, 2);
  assert.equal(h.sockets[0].sent.length, 1);
  h.client.dispose();
});

test('malformed send results cannot confirm a save and trigger authoritative repair', async () => {
  const h = harness();
  h.client.start(); h.client.select(a); h.sockets[0].open();
  h.requests[0].resolve(page()); await flush();
  h.client.send('hello');
  h.sockets[0].message({ event: 'send_result', client_message_id: 'stable-request',
    result: { client_message_id: 'stable-request', stored: true, message: message('M1', '1'), dispatch: 'delivered', replayed: false } });
  assert.equal(h.client.state.snapshot(a.id)[0].status, 'uncertain');
  assert.equal(h.requests.length, 2);
  h.client.dispose();
});

test('reauthentication stops reconnect and immediately marks every in-flight send uncertain', async () => {
  const h = harness();
  h.client.start(); h.client.select(a); h.sockets[0].open();
  h.requests[0].resolve(page()); await flush();
  h.client.send('hello');
  h.client.uuid = () => 'other-request';
  h.client.send('second');
  h.sockets[0].message({ event: 'send_result', client_message_id: 'stable-request',
    error: { code: 'unauthenticated', retry: 'reauthenticate' } });
  assert.equal(h.changes.at(-1).authRequired, true);
  assert.deepEqual(h.client.state.snapshot(a.id).map(m => m.status), ['uncertain', 'uncertain']);
  await h.clock.tick(60000);
  assert.equal(h.sockets.length, 1);
  assert.equal(h.changes.at(-1).connected, false);
  h.client.dispose();
});
