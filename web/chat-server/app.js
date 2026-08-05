const API = 'http://localhost:8080';
const WS = 'ws://localhost:8080';
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
  if (!response.ok) throw new Error(body.error || body.message || body.msg || `HTTP ${response.status}`);
  return body.data ?? body;
}

function enterChat() {
  show('auth-page', false);
  show('chat-page', true);
  loadMe().then(loadSessions).catch(err => message('chat-message', err.message));
}

async function loadMe() {
  me = await request('/user/getUserInfo', { method: 'POST', body: JSON.stringify({}) });
  $('my-name').textContent = me.nickname || me.Nickname || '当前用户';
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
      message('auth-message', '注册成功，请登录');
      toggleAuth();
      return;
    }
    token = result.token || result.Token;
    localStorage.setItem('token', token);
    enterChat();
  } catch (err) { message('auth-message', err.message); }
}

async function loadSessions() {
  const result = await request('/session/getUserSessionList', { method: 'POST', body: JSON.stringify({}) });
  const list = Array.isArray(result) ? result : (result.list || result.sessions || []);
  $('sessions').innerHTML = '';
  list.forEach(session => {
    const id = session.user_id || session.userId || session.receive_id || session.uuid;
    const button = document.createElement('button');
    button.className = 'session';
    button.textContent = session.user_name || session.nickname || id;
    button.onclick = () => openSession({ ...session, user_id: id }, button);
    $('sessions').appendChild(button);
  });
}

async function openSession(session, button) {
  currentSession = session;
  $('message-input').disabled = false;
  $('message-form').querySelector('button').disabled = false;
  document.querySelectorAll('.session').forEach(item => item.classList.remove('active'));
  button?.classList.add('active');
  $('chat-title').textContent = session.user_name || session.nickname || session.user_id;
  await loadMessages();
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
  socket = new WebSocket(`${WS}/wss?token=${encodeURIComponent(token)}`);
  socket.onopen = () => $('connection').textContent = '已连接';
  socket.onclose = () => $('connection').textContent = '未连接';
  socket.onerror = () => message('chat-message', 'WebSocket 连接失败');
  socket.onmessage = event => addMessage(JSON.parse(event.data));
}

function sendMessage(event) {
  event.preventDefault();
  const content = $('message-input').value.trim();
  if (!content || !currentSession || !socket || socket.readyState !== WebSocket.OPEN) return;
  socket.send(JSON.stringify({ type: 'text', content, receive_id: currentSession.user_id }));
  $('message-input').value = '';
}

function toggleAuth() {
  registerMode = !registerMode;
  $('nickname').hidden = !registerMode;
  $('auth-submit').textContent = registerMode ? '注册' : '登录';
  $('toggle-auth').textContent = registerMode ? '已有账号？登录' : '没有账号？注册';
  message('auth-message', '');
}

$('auth-form').onsubmit = login;
$('toggle-auth').onclick = toggleAuth;
$('refresh-sessions').onclick = () => loadSessions().catch(err => message('chat-message', err.message));
$('open-session-form').onsubmit = async event => {
  event.preventDefault();
  try {
    const uuid = $('target-uuid').value.trim();
    await request('/session/openSession', { method: 'POST', body: JSON.stringify({ receive_id: uuid }) });
    await loadSessions();
  } catch (err) { message('chat-message', err.message); }
};
$('message-form').onsubmit = sendMessage;
$('logout').onclick = () => { socket?.close(); localStorage.removeItem('token'); location.reload(); };

if (token) enterChat();
