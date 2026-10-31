const dispatches = new Set(['queued', 'not_queued', 'unknown']);
const retries = new Set(['do_not_retry', 'same_request', 'reconnect', 'reauthenticate', 'refresh_route']);
const errors = new Set(['invalid_argument', 'idempotency_conflict', 'unauthenticated',
  'permission_denied', 'superseded', 'lease_expired', 'instance_not_registered',
  'dependency_unavailable', 'resource_exhausted', 'draining', 'result_unknown']);
const text = value => typeof value === 'string' && value.length > 0;
const position = value => typeof value === 'string' && /^[1-9][0-9]{0,18}$/.test(value)
  && BigInt(value) <= 9223372036854775807n;

export function conversationFromSession(session) {
  if (!session || typeof session !== 'object') return null;
  const peer = session.peer_uuid || session.PeerUUID || session.user_id;
  const id = session.session_uuid || session.SessionUUID;
  if (typeof peer !== 'string') return null;
  if (peer.startsWith('G')) return { id: `group:${peer}`, receive_id: peer, receive_type: 'group' };
  if (peer.startsWith('U') && typeof id === 'string' && id.startsWith('S')) {
    return { id: `direct:${id}`, receive_id: peer, receive_type: 'user' };
  }
  return null;
}

function validMessage(m) {
  if (!m || !['message_id', 'session_id', 'send_id', 'receive_id', 'conversation_id', 'content'].every(k => text(m[k]))
    || !position(m.position) || m.type !== 'text' || !['user', 'ai'].includes(m.origin)
    || typeof m.created_at !== 'string' || !Number.isFinite(Date.parse(m.created_at))
    || new TextEncoder().encode(m.content).length > 16 * 1024) return false;
  return m.receive_type === 'user'
    ? m.conversation_id === `direct:${m.session_id}` && m.session_id.startsWith('S') && m.receive_id.startsWith('U')
    : m.receive_type === 'group' && m.conversation_id === `group:${m.session_id}`
      && m.session_id.startsWith('G') && m.receive_id === m.session_id;
}

function validFrame(f) {
  if (!f || typeof f !== 'object') return false;
  if (f.event === 'message') return validMessage(f.message);
  if (f.event !== 'send_result' || !text(f.client_message_id) || Boolean(f.result) === Boolean(f.error)) return false;
  if (f.error) return errors.has(f.error.code) && retries.has(f.error.retry);
  return f.result.client_message_id === f.client_message_id && f.result.stored === true
    && typeof f.result.replayed === 'boolean' && dispatches.has(f.result.dispatch) && validMessage(f.result.message);
}

export function parseFrame(raw) {
  // Go JSON can escape each ASCII content byte as six characters (e.g. < → \u003c).
  // Allow the full encoded 16 KiB message plus envelope; validate decoded bytes below.
  if (typeof raw !== 'string' || raw.length > 128 * 1024) return null;
  try {
    const frame = JSON.parse(raw);
    return validFrame(frame) ? frame : null;
  } catch { return null; }
}

function merge(chat, message) {
  const old = chat.saved.get(message.message_id);
  chat.saved.set(message.message_id, { ...message, ...(old?.delivery ? { delivery: old.delivery } : {}) });
}

export class ChatState {
  constructor(userID, { maxPending = 100 } = {}) {
    this.userID = userID;
    this.maxPending = maxPending;
    this.chats = new Map();
  }

  select(conversation) {
    if (!this.chats.has(conversation.id)) this.chats.set(conversation.id, {
      ...conversation, saved: new Map(), pending: new Map(), buffer: new Map(), cursor: '', syncing: false,
    });
  }

  snapshot(id) {
    const chat = this.chats.get(id);
    if (!chat) return [];
    const saved = [...chat.saved.values()].sort((a, b) => {
      const left = BigInt(a.position), right = BigInt(b.position);
      return left < right ? -1 : left > right ? 1 : a.message_id.localeCompare(b.message_id);
    });
    return [...saved, ...chat.pending.values()];
  }

  beginSync(id) {
    const chat = this.chats.get(id);
    chat.syncing = true;
    return chat.cursor;
  }

  mergePage(id, page) {
    const chat = this.chats.get(id);
    if (!chat || !page || page.conversation_id !== id || !Array.isArray(page.messages)
      || typeof page.next_cursor !== 'string' || typeof page.has_more !== 'boolean'
      || (page.messages.length === 0 ? page.next_cursor !== chat.cursor : page.next_cursor === '')
      || (page.has_more && (page.messages.length === 0 || page.next_cursor === chat.cursor))
      || !page.messages.every(m => validMessage(m) && m.conversation_id === id)) return false;
    for (const message of page.messages) merge(chat, message);
    // This is the sole cursor assignment: only an entirely validated, merged page advances it.
    chat.cursor = page.next_cursor;
    return true;
  }

  finishSync(id) {
    const chat = this.chats.get(id);
    if (!chat) return;
    for (const m of chat.buffer.values()) merge(chat, m);
    chat.buffer.clear();
    chat.syncing = false;
  }

  receive(message) {
    if (!validMessage(message)) return false;
    const chat = this.chats.get(message.conversation_id);
    if (!chat) return false;
    if (chat.syncing) {
      chat.buffer.set(message.message_id, message);
      // Bound the paging buffer without dropping realtime or moving the sync cursor.
      if (chat.buffer.size >= 1000) {
        for (const m of chat.buffer.values()) merge(chat, m);
        chat.buffer.clear();
      }
    } else merge(chat, message);
    return true;
  }

  createPending(id, content, clientID) {
    const chat = this.chats.get(id);
    if (!chat || !text(content) || new TextEncoder().encode(content).length > 16 * 1024) throw Error('Message must contain 1–16384 UTF-8 bytes');
    if (this.findPending(clientID)) throw Error('Request ID already exists');
    if ([...this.chats.values()].reduce((n, c) => n + c.pending.size, 0) >= this.maxPending) throw Error('Too many pending messages; resolve existing requests first');
    const frame = Object.freeze({ event: 'send_message', client_message_id: clientID,
      receive_id: chat.receive_id, receive_type: chat.receive_type, content });
    chat.pending.set(clientID, { ...frame, conversation_id: id, send_id: this.userID,
      status: 'pending', retryable: true, frame });
    return frame;
  }

  findPending(clientID) {
    for (const chat of this.chats.values()) {
      const item = chat.pending.get(clientID);
      if (item) return { chat, item };
    }
    return null;
  }

  retry(clientID) {
    const pending = this.findPending(clientID);
    if (!pending || !pending.item.retryable || pending.item.status === 'pending') return null;
    pending.item.status = 'pending';
    delete pending.item.error;
    return pending.item.frame;
  }

  uncertain(clientID) {
    const pending = this.findPending(clientID);
    if (pending) pending.item.status = 'uncertain';
  }

  result(frame) {
    if (!validFrame(frame) || frame.event !== 'send_result') return false;
    const pending = this.findPending(frame.client_message_id);
    if (!pending) return false;
    const { chat, item } = pending;
    if (frame.error) {
      item.status = 'uncertain';
      item.error = frame.error.code;
      item.retryable = !['do_not_retry', 'reauthenticate'].includes(frame.error.retry);
      return true;
    }
    const { message, dispatch } = frame.result;
    if (message.conversation_id !== chat.id || message.send_id !== this.userID
      || message.content !== item.content || message.receive_id !== item.receive_id
      || message.receive_type !== item.receive_type || message.origin !== 'user') return false;
    merge(chat, { ...message, delivery: dispatch });
    // Replace any previous receipt with this known server receipt, then remove optimism.
    chat.saved.get(message.message_id).delivery = dispatch;
    chat.pending.delete(frame.client_message_id);
    return true;
  }
}
