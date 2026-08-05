const API = '';
const WS = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}`;
let token = localStorage.getItem('token') || '';
let me = null;
let currentSession = null;
let socket = null;
let registerMode = false;

const $ = id => document.getElementById(id);
const show = (id, value) => $(id).hidden = !value;
const message = (id, text) => $(id).textContent = text || '';

async function request(path, options = {}) {
  const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
  if (token) headers.Authorization = `Bearer ${token}`;
  const response = await fetch(API + path, { ...options, headers });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(body.error || body.message || body.msg || `HTTP ${response.status}`);
    error.status = response.status;
    throw error;
  }
  return body.data ?? body;
}

function tokenUserUUID() {
  if (!token) return '';
  const payload = token.split('.')[1];
  if (!payload) return '';
  const base64 = payload.replace(/-/g, '+').replace(/_/g, '/');
  const padded = base64.padEnd(Math.ceil(base64.length / 4) * 4, '=');
  return JSON.parse(atob(padded)).user_uuid || '';
}

function enterChat() {
  show('auth-page', false);
  show('chat-page', true);
  loadMe().then(loadSessions).catch(err => message('chat-message', err.message));
}

async function loadMe() {
  me = await request('/user/getUserInfo', {
    method: 'POST',
    body: JSON.stringify({ uuid: tokenUserUUID() })
  });
  $('my-name').textContent = me.nickname || me.Nickname || 'Current user';
}

async function login(event) {
  event.preventDefault();
  const body = { telephone: $('telephone').value, password: $('password').value };
  if (registerMode) body.nickname = $('nickname').value;
  try {
    const result = await request(registerMode ? '/register' : '/login', {
      method: 'POST', body: JSON.stringify(body), headers: {}
    });
    if (registerMode) {
      message('auth-message', 'Registered. Please log in.');
      toggleAuth();
      return;
    }
    token = result.token || result.Token;
    if (!token) throw new Error('Login response has no token');
    localStorage.setItem('token', token);
    enterChat();
  } catch (err) {
    message('auth-message', err.message);
  }
}

async function loadSessions() {
  const result = await request('/session/getUserSessionList', {
    method: 'POST', body: JSON.stringify({})
  });
  const list = Array.isArray(result) ? result : (result.list || result.sessions || []);
  $('sessions').innerHTML = '';
  list.forEach(session => {
    const peerUUID = session.peer_uuid || session.PeerUUID;
    if (!peerUUID) return;
    const button = document.createElement('button');
    button.className = 'session';
    button.textContent = peerUUID;
    button.onclick = () => openSession({ ...session, user_id: peerUUID }, button);
    $('sessions').appendChild(button);
  });
}

async function openSession(session, button) {
  currentSession = session;
  $('message-input').disabled = false;
  $('message-form').querySelector('button').disabled = false;
  document.querySelectorAll('.session').forEach(item => item.classList.remove('active'));
  button?.classList.add('active');
  $('chat-title').textContent = session.user_id;
  try {
    await loadMessages();
  } catch (err) {
    message('chat-message', err.message);
    return;
  }
  connectSocket();
}

async function loadMessages() {
  const result = await request('/message/getMessageList', {
    method: 'POST',
    body: JSON.stringify({ user_one_id: me.uuid, user_two_id: currentSession.user_id })
  });
  const list = Array.isArray(result) ? result : (result.list || result.messages || []);
  $('messages').innerHTML = '';
  list.forEach(addMessage);
}

function addMessage(item) {
  const node = document.createElement('div');
  node.className = 'msg' + ((item.send_id || item.from) === me?.uuid ? ' mine' : '');
  node.textContent = item.content || item.message || '';
  $('messages').appendChild(node);
  $('messages').scrollTop = $('messages').scrollHeight;
}

function connectSocket() {
  socket?.close();
  const params = new URLSearchParams({ token, client_id: me.uuid });
  socket = new WebSocket(`${WS}/wss?${params}`);
  const activeSocket = socket;
  socket.onopen = () => {
    if (socket !== activeSocket) return;
    $('connection').textContent = 'Connected';
    message('chat-message', '');
  };
  socket.onclose = () => {
    if (socket === activeSocket) $('connection').textContent = 'Disconnected';
  };
  socket.onerror = () => {
    if (socket === activeSocket) message('chat-message', 'WebSocket connection failed');
  };
  socket.onmessage = event => {
    const item = JSON.parse(event.data);
    if (!currentSession || item.send_id !== currentSession.user_id) return;
    addMessage(item);
  };
}

function sendMessage(event) {
  event.preventDefault();
  const content = $('message-input').value.trim();
  if (!content || !currentSession) return;
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    message('chat-message', 'The chat is not connected yet. Please wait and try again.');
    return;
  }
  socket.send(JSON.stringify({ send_id: me.uuid, receive_id: currentSession.user_id, content }));
  $('message-input').value = '';
  addMessage({ send_id: me.uuid, content });
}

function toggleAuth() {
  registerMode = !registerMode;
  $('nickname').hidden = !registerMode;
  $('auth-submit').textContent = registerMode ? 'Register' : 'Login';
  $('toggle-auth').textContent = registerMode ? 'Have an account? Login' : 'Need an account? Register';
  message('auth-message', '');
}

$('auth-form').onsubmit = login;
$('toggle-auth').onclick = toggleAuth;
$('refresh-sessions').onclick = () => loadSessions().catch(err => message('chat-message', err.message));
$('open-session-form').onsubmit = async event => {
  event.preventDefault();
  try {
    const peerUUID = $('target-uuid').value.trim();
    await request('/session/openSession', {
      method: 'POST', body: JSON.stringify({ peer_uuid: peerUUID })
    });
    await loadSessions();
    const sessionButton = [...document.querySelectorAll('.session')]
      .find(button => button.textContent === peerUUID);
    sessionButton?.click();
  } catch (err) {
    message('chat-message', err.message);
  }
};
$('message-form').onsubmit = sendMessage;
$('logout').onclick = () => {
  socket?.close();
  localStorage.removeItem('token');
  location.reload();
};

if (token) enterChat();
