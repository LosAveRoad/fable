import { ChatState, parseFrame } from './chat-state.js';

function requestUUID() {
  const crypto = globalThis.crypto;
  if (crypto?.randomUUID) return crypto.randomUUID();
  if (!crypto?.getRandomValues) throw Error('Secure message IDs require HTTPS or localhost');
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 15) | 64;
  bytes[8] = (bytes[8] & 63) | 128;
  const hex = [...bytes].map(b => b.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

// Owns one authenticated subscription for the entire signed-in view. HTTP and
// sockets are injected only at the external boundary, so the lifecycle is testable.
export class ChatClient {
  constructor({ userID, token, wsBase, createSocket = url => new WebSocket(url), fetchPage,
    onChange = () => {}, onError = () => {}, uuid = requestUUID, clock = globalThis }) {
    this.state = new ChatState(userID);
    Object.assign(this, { token, wsBase, createSocket, fetchPage, onChange, onError, uuid, clock });
    this.timers = new Set();
    this.pendingTimers = new Map();
    this.connected = false;
    this.disposed = false;
    this.authRequired = false;
    this.reconnectDelay = 1000;
    this.syncDelay = 1000;
  }

  later(callback, delay) {
    if (this.disposed) return;
    const id = this.clock.setTimeout(() => {
      this.timers.delete(id);
      if (!this.disposed) callback();
    }, delay);
    this.timers.add(id);
    return id;
  }

  cancel(id) {
    this.clock.clearTimeout(id);
    this.timers.delete(id);
  }

  emit() {
    if (!this.disposed) this.onChange({ conversationID: this.current?.id || null,
      messages: this.state.snapshot(this.current?.id), connected: this.connected,
      syncing: Boolean(this.syncTask), authRequired: this.authRequired });
  }

  start() {
    if (this.started || this.disposed) return;
    this.started = true;
    this.connect();
    const periodic = () => {
      this.sync();
      this.later(periodic, 30000);
    };
    this.later(periodic, 30000);
  }

  connect() {
    if (this.disposed || this.authRequired) return;
    let socket;
    try {
      const url = new URL(`${this.wsBase.replace(/\/$/, '')}/wss`);
      url.searchParams.set('protocol', 'v2');
      url.searchParams.set('token', this.token);
      socket = this.createSocket(url.toString());
    } catch {
      this.onError('Live connection failed; retrying');
      this.reconnect();
      return;
    }
    this.socket = socket;
    const live = () => !this.disposed && this.socket === socket;
    socket.onopen = () => {
      if (!live()) return;
      this.connected = true;
      this.reconnectDelay = 1000;
      this.emit();
      this.sync();
    };
    socket.onmessage = event => {
      if (live()) this.receive(event.data);
    };
    socket.onerror = () => {
      if (live()) this.onError('Live connection interrupted');
    };
    socket.onclose = () => {
      if (!live()) return;
      this.socket = null;
      this.connected = false;
      this.abortSync();
      this.markUncertain();
      this.emit();
      this.reconnect();
    };
  }

  reconnect() {
    if (this.disposed || this.authRequired) return;
    this.cancel(this.reconnectTimer);
    this.reconnectTimer = this.later(() => this.connect(), this.reconnectDelay);
    this.reconnectDelay = Math.min(30000, this.reconnectDelay * 2);
  }

  select(conversation) {
    if (this.disposed) return;
    this.abortSync();
    this.cancel(this.syncTimer);
    this.current = conversation;
    this.state.select(conversation);
    this.syncDelay = 1000;
    this.emit();
    this.sync();
  }

  abortSync() {
    const task = this.syncTask;
    if (!task) return;
    task.controller.abort();
    this.cancel(task.deadline);
    this.state.finishSync(task.id);
    this.syncTask = null;
  }

  async sync() {
    if (this.disposed || !this.connected || !this.current || this.authRequired) return;
    if (this.syncTask) { this.syncTask.again = true; return; }
    this.cancel(this.syncTimer);
    const task = { id: this.current.id, controller: new AbortController(), again: false };
    this.syncTask = task;
    let cursor = this.state.beginSync(task.id), retryDelay = null;
    const current = () => !this.disposed && this.syncTask === task && this.current?.id === task.id
      && !task.controller.signal.aborted;
    this.emit();
    try {
      // Yield after a bounded batch so an enormous history cannot starve live UI.
      for (let pages = 0; pages < 20; pages++) {
        task.deadline = this.later(() => task.controller.abort(), 15000);
        const page = await this.fetchPage(task.id, cursor, task.controller.signal);
        this.cancel(task.deadline);
        if (!current()) return;
        if (!this.state.mergePage(task.id, page)) throw Error('Invalid synchronization response');
        cursor = page.next_cursor;
        this.emit();
        if (!page.has_more) break;
        if (pages === 19) task.again = true;
      }
      this.syncDelay = 1000;
      if (task.again) retryDelay = 0;
    } catch (error) {
      if (this.disposed || this.syncTask !== task) return;
      this.onError(error.name === 'AbortError' ? 'History synchronization timed out; retrying' : 'History synchronization failed; retrying');
      retryDelay = this.syncDelay;
      this.syncDelay = Math.min(30000, this.syncDelay * 2);
    } finally {
      this.cancel(task.deadline);
      if (this.syncTask === task) {
        this.state.finishSync(task.id);
        this.syncTask = null;
        this.emit();
        if (retryDelay !== null) this.syncTimer = this.later(() => this.sync(), retryDelay);
      }
    }
  }

  markUncertain() {
    for (const [id, timer] of this.pendingTimers) {
      this.cancel(timer);
      this.state.uncertain(id);
    }
    this.pendingTimers.clear();
  }

  receive(raw) {
    const frame = parseFrame(raw);
    if (!frame) {
      this.markUncertain();
      this.onError('Unrecognized live response; checking saved history');
      this.emit();
      this.sync();
      return;
    }
    if (frame.event === 'message') {
      this.state.receive(frame.message);
    } else {
      const pending = this.state.findPending(frame.client_message_id);
      if (!pending) return; // Late/duplicate receipts do not create a new bubble.
      this.cancel(this.pendingTimers.get(frame.client_message_id));
      this.pendingTimers.delete(frame.client_message_id);
      if (!this.state.result(frame)) {
        this.state.uncertain(frame.client_message_id);
        this.onError('Send response did not match the request; checking saved history');
        this.sync();
      } else if (frame.error || frame.result.dispatch !== 'queued') {
        this.sync();
      }
      if (frame.error?.retry === 'reauthenticate') {
        this.authRequired = true;
        this.connected = false;
        this.abortSync();
        this.markUncertain();
        this.closeSocket();
        this.onError('Please log out and sign in again. Previous sends may already be saved.');
      } else if (['reconnect', 'refresh_route'].includes(frame.error?.retry)) {
        this.socket?.close();
      }
    }
    this.emit();
  }

  send(content) {
    if (this.disposed || !this.current || !this.connected || this.socket?.readyState !== 1) throw Error('Wait for the live connection before sending');
    const frame = this.state.createPending(this.current.id, content, this.uuid());
    this.transmit(frame);
  }

  retry(clientID) {
    if (this.disposed || !this.connected || this.socket?.readyState !== 1) throw Error('Wait for the live connection before retrying');
    const pending = this.state.findPending(clientID);
    if (!pending || pending.chat.id !== this.current?.id) return;
    const frame = this.state.retry(clientID);
    if (frame) this.transmit(frame);
  }

  transmit(frame) {
    const id = frame.client_message_id;
    this.cancel(this.pendingTimers.get(id));
    try {
      this.socket.send(JSON.stringify(frame));
      this.pendingTimers.set(id, this.later(() => {
        this.pendingTimers.delete(id);
        this.state.uncertain(id);
        this.emit();
        this.sync();
      }, 12000));
    } catch {
      this.state.uncertain(id);
      this.onError('Send result unknown; check history or retry this request');
      this.sync();
    }
    this.emit();
  }

  closeSocket() {
    if (!this.socket) return;
    const socket = this.socket;
    this.socket = null;
    socket.onopen = socket.onmessage = socket.onerror = socket.onclose = null;
    socket.close();
  }

  dispose() {
    if (this.disposed) return;
    this.disposed = true;
    this.abortSync();
    this.closeSocket();
    for (const id of this.timers) this.clock.clearTimeout(id);
    this.timers.clear();
    this.pendingTimers.clear();
    this.state.chats.clear();
    this.token = '';
    this.connected = false;
  }
}
