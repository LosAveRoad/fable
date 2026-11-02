import test from 'node:test';
import assert from 'node:assert/strict';
import { ChatState, conversationFromSession, parseFrame } from './chat-state.js';

const direct = { id: 'direct:S1', receive_id: 'U2', receive_type: 'user' };
const message = (id = 'M1', position = '1', extra = {}) => ({
  message_id: id, conversation_id: 'direct:S1', session_id: 'S1', send_id: 'U1',
  receive_id: 'U2', receive_type: 'user', type: 'text', content: 'hello', origin: 'user',
  created_at: '2026-10-31T00:00:00Z', position, ...extra,
});
const page = (messages, next_cursor = 'page-1', has_more = false) => ({
  conversation_id: direct.id, messages, next_cursor, has_more,
});
const ack = (client_message_id = 'request-1', dispatch = 'queued') => ({
  event: 'send_result', client_message_id,
  result: { client_message_id, stored: true, message: message(), dispatch, replayed: false },
});
function setup(options) {
  const state = new ChatState('U1', options);
  state.select(direct);
  return state;
}

test('maps direct session UUID and group peer UUID to authoritative conversations', () => {
  assert.deepEqual(conversationFromSession({ session_uuid: 'S1', peer_uuid: 'U2' }), direct);
  assert.deepEqual(conversationFromSession({ session_uuid: 'S2', peer_uuid: 'G1' }), {
    id: 'group:G1', receive_id: 'G1', receive_type: 'group',
  });
  assert.equal(conversationFromSession({ peer_uuid: 'U2' }), null);
});

test('realtime before and during history merges once without advancing authoritative cursor', () => {
  const state = setup();
  state.receive(message('M3', '3'));
  assert.equal(state.beginSync(direct.id), '');
  state.receive(message('M2', '2'));
  state.mergePage(direct.id, page([message('M1', '1'), message('M2', '2')], 'cursor-2', true));
  assert.equal(state.chats.get(direct.id).cursor, 'cursor-2');
  state.receive(message('M4', '4'));
  state.finishSync(direct.id);
  assert.deepEqual(state.snapshot(direct.id).map(m => m.message_id), ['M1', 'M2', 'M3', 'M4']);
  assert.equal(state.chats.get(direct.id).cursor, 'cursor-2');
});

for (const order of ['ack-first', 'push-first']) {
  test(`${order} reconciles the optimistic bubble by message_id`, () => {
    const state = setup();
    state.createPending(direct.id, 'hello', 'request-1');
    if (order === 'ack-first') state.result(ack());
    state.receive(message());
    if (order === 'push-first') state.result(ack());
    assert.equal(state.snapshot(direct.id).length, 1);
    assert.equal(state.snapshot(direct.id)[0].message_id, 'M1');
    assert.equal(state.snapshot(direct.id)[0].delivery, 'queued');
    assert.equal(state.chats.get(direct.id).cursor, '');
  });
}

test('retry preserves the exact command and ID after uncertainty', () => {
  const state = setup();
  const command = state.createPending(direct.id, 'hello', 'request-1');
  assert.deepEqual(command, { event: 'send_message', client_message_id: 'request-1',
    receive_id: 'U2', receive_type: 'user', content: 'hello' });
  state.uncertain('request-1');
  assert.deepEqual(state.retry('request-1'), command);
  assert.equal(state.snapshot(direct.id).length, 1);
});

test('orders positions above 2^53 exactly and keeps unsaved items last', () => {
  const state = setup();
  state.createPending(direct.id, 'later', 'request-2');
  for (const [id, position] of [['M4', '9223372036854775807'], ['M2', '9007199254740993'], ['M1', '9007199254740992']]) {
    state.receive(message(id, position));
  }
  assert.deepEqual(state.snapshot(direct.id).map(m => m.message_id || m.client_message_id), ['M1', 'M2', 'M4', 'request-2']);
});

test('invalid sync pages are atomic and failed paging retains buffered realtime for retry', () => {
  const state = setup();
  state.beginSync(direct.id);
  state.receive(message('M3', '3'));
  assert.equal(state.mergePage(direct.id, page([message(), message('bad', 4)])), false);
  assert.equal(state.chats.get(direct.id).cursor, '');
  state.finishSync(direct.id);
  assert.deepEqual(state.snapshot(direct.id).map(m => m.message_id), ['M3']);
  assert.equal(state.beginSync(direct.id), '');
  assert.equal(state.mergePage(direct.id, page([message(), message('M3', '3')], 'cursor-3')), true);
  state.finishSync(direct.id);
  assert.deepEqual(state.snapshot(direct.id).map(m => m.message_id), ['M1', 'M3']);
});

test('an empty page cannot move the cursor and a nonempty page must return one', () => {
  const state = setup();
  state.beginSync(direct.id);
  assert.equal(state.mergePage(direct.id, page([], 'unearned-cursor')), false);
  assert.equal(state.chats.get(direct.id).cursor, '');
  assert.equal(state.mergePage(direct.id, page([message()], '')), false);
  assert.equal(state.snapshot(direct.id).length, 0);
});

test('malformed session rows are ignored without preventing other conversations from loading', () => {
  assert.equal(conversationFromSession(null), null);
  assert.equal(conversationFromSession({ session_uuid: 'S1', peer_uuid: 123 }), null);
});

test('unknown dispatch and malformed frames cannot mark an optimistic request saved', () => {
  const state = setup();
  state.createPending(direct.id, 'hello', 'request-1');
  assert.equal(parseFrame('{broken'), null);
  assert.equal(parseFrame(JSON.stringify({ event: 'message', message: message('M1', 9007199254740993) })), null);
  assert.equal(parseFrame(JSON.stringify(ack('request-1', 'delivered'))), null);
  assert.equal(state.result(ack('request-1', 'delivered')), false);
  assert.equal(state.snapshot(direct.id)[0].message_id, undefined);
  assert.equal(parseFrame(JSON.stringify({ ...ack(), error: { code: 'result_unknown', retry: 'same_request' } })), null);
  assert.equal(parseFrame(JSON.stringify({ ...ack(), result: { ...ack().result, client_message_id: 'wrong' } })), null);
});

test('permission denied after retry remains save-unconfirmed and blocks forbidden retries', () => {
  const state = setup();
  state.createPending(direct.id, 'hello', 'request-1');
  state.uncertain('request-1');
  state.retry('request-1');
  state.result({ event: 'send_result', client_message_id: 'request-1',
    error: { code: 'permission_denied', retry: 'do_not_retry' } });
  const item = state.snapshot(direct.id)[0];
  assert.equal(item.status, 'uncertain');
  assert.equal(item.error, 'permission_denied');
  assert.equal(item.retryable, false);
  assert.equal(state.retry('request-1'), null);
});

test('stored dispatch unknown preserves saved message and AI origin from authoritative frames', () => {
  const state = setup();
  state.createPending(direct.id, 'hello', 'request-1');
  assert.equal(state.result(ack('request-1', 'unknown')), true);
  state.receive(message('M2', '2', { origin: 'ai' }));
  assert.equal(state.snapshot(direct.id)[0].delivery, 'unknown');
  assert.equal(state.snapshot(direct.id)[1].origin, 'ai');
});

test('bounded pending preserves existing retry identity and rejects overflow', () => {
  const state = setup({ maxPending: 2 });
  state.createPending(direct.id, 'one', 'request-1');
  state.createPending(direct.id, 'two', 'request-2');
  assert.throws(() => state.createPending(direct.id, 'three', 'request-3'), /pending/i);
  state.uncertain('request-1');
  assert.equal(state.retry('request-1').content, 'one');
});

test('Go-escaped maximum-size push and retry receipt reconcile to one saved bubble', () => {
  const state = setup();
  const content = '<'.repeat(16384);
  const saved = message('M1', '1', { content });
  const goJSON = value => JSON.stringify(value).replaceAll('<', '\\u003c');
  state.createPending(direct.id, content, 'request-1');
  const push = parseFrame(goJSON({ event: 'message', message: saved }));
  assert.notEqual(push, null, 'a valid escaped 16 KiB push must be accepted');
  state.receive(push.message);
  state.mergePage(direct.id, page([saved]));
  state.uncertain('request-1');
  assert.equal(state.retry('request-1').content, content);
  const receipt = parseFrame(goJSON({ ...ack(), result: { ...ack().result, message: saved } }));
  assert.notEqual(receipt, null, 'a valid escaped 16 KiB receipt must be accepted');
  assert.equal(state.result(receipt), true);
  const messages = state.snapshot(direct.id);
  assert.equal(messages.length, 1);
  assert.equal(messages[0].message_id, 'M1');
  assert.equal(messages[0].content.length, 16384);
  assert.equal(messages[0].delivery, 'queued');
});

test('escaped frames still reject content exceeding the decoded UTF-8 byte limit', () => {
  for (const content of ['<'.repeat(16385), '😀'.repeat(4097)]) {
    const saved = message('M1', '1', { content });
    const encode = value => JSON.stringify(value).replaceAll('<', '\\u003c');
    assert.equal(parseFrame(encode({ event: 'message', message: saved })), null);
    assert.equal(parseFrame(encode({ ...ack(), result: { ...ack().result, message: saved } })), null);
  }
});
