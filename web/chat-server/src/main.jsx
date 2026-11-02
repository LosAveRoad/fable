import React, { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  ArrowLeft,
  LogOut,
  Menu,
  MessageCircle,
  Plus,
  RefreshCw,
  Send,
  Sparkles,
  Wifi,
  WifiOff,
  X,
} from "lucide-react";
import "./styles.css";
import logoUrl from "../fable-im-logo.png";
import { ChatClient } from "./chat-client.js";
import { conversationFromSession } from "./chat-state.js";

const API = import.meta.env.VITE_API_BASE || "";
const WS =
  import.meta.env.VITE_WS_BASE ||
  `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}`;
const api = async (path, token, options = {}) => {
  const r = await fetch(API + path, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      Authorization: token ? `Bearer ${token}` : "",
    },
  });
  const b = await r.json().catch(() => ({}));
  if (!r.ok)
    throw Error(b.error || b.message || `Request failed (${r.status})`);
  return b.data ?? b;
};
const uuidFrom = (token = "") => {
  try {
    return (
      JSON.parse(
        atob(token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")),
      ).user_uuid || ""
    );
  } catch {
    return "";
  }
};

function Auth({ onLogin }) {
  const [register, setRegister] = useState(false),
    [form, setForm] = useState({ nickname: "", telephone: "", password: "" }),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false);
  const change = (e) => setForm({ ...form, [e.target.name]: e.target.value });
  const submit = async (e) => {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const result = await api(register ? "/register" : "/login", "", {
        method: "POST",
        body: JSON.stringify(form),
      });
      if (register) {
        setRegister(false);
        setError("Account created — you can log in now.");
      } else {
        const token = result.token || result.Token;
        if (!token) throw Error("Login did not return a token");
        onLogin(token);
      }
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <main className="auth-shell">
      <section className="auth-card">
        <div className="logo-lockup">
          <img src={logoUrl} alt="Fable IM" />
          <span>FABLE / IM</span>
        </div>
        <h1>{register ? "Create your account" : "Welcome back"}</h1>
        <p className="muted">A calm space for conversations that matter.</p>
        <form onSubmit={submit}>
          {register && (
            <input
              name="nickname"
              placeholder="Nickname"
              required
              value={form.nickname}
              onChange={change}
            />
          )}
          <input
            name="telephone"
            placeholder="Telephone"
            required
            value={form.telephone}
            onChange={change}
          />
          <input
            name="password"
            type="password"
            placeholder="Password"
            required
            value={form.password}
            onChange={change}
          />
          <button className="primary" disabled={busy}>
            {busy ? "Please wait…" : register ? "Register" : "Login"}{" "}
            <ArrowLeft size={17} className="flip" />
          </button>
        </form>
        <button
          className="text-button"
          onClick={() => {
            setRegister(!register);
            setError("");
          }}
        >
          {register ? "Have an account? Login" : "Need an account? Register"}
        </button>
        {error && <p className="notice">{error}</p>}
      </section>
    </main>
  );
}

function Chat({ token, onLogout }) {
  const [me, setMe] = useState(null),
    [sessions, setSessions] = useState([]),
    [current, setCurrent] = useState(null),
    [view, setView] = useState({ messages: [], connected: false, syncing: false, authRequired: false }),
    [error, setError] = useState(""),
    [peer, setPeer] = useState(""),
    [mobileOpen, setMobileOpen] = useState(false);
  const client = useRef(null), alive = useRef(false), requests = useRef(null), listVersion = useRef(0);
  const userID = uuidFrom(token);
  const { messages, connected, syncing, authRequired } = view;
  useEffect(() => {
    alive.current = true;
    const controller = new AbortController();
    requests.current = controller;
    const live = new ChatClient({
      userID, token, wsBase: WS,
      fetchPage: (conversation, cursor, signal) => {
        const query = new URLSearchParams({ conversation_id: conversation, cursor, limit: "100" });
        return api(`/api/v2/messages/sync?${query}`, token, { signal });
      },
      onChange: snapshot => { if (alive.current) setView(snapshot); },
      onError: message => { if (alive.current) setError(message); },
    });
    client.current = live;
    live.start();
    (async () => {
      try {
        const user = await api("/user/getUserInfo", token, {
          method: "POST",
          body: JSON.stringify({ uuid: userID }),
          signal: controller.signal,
        });
        if (!alive.current) return;
        setMe(user);
        await refresh();
      } catch (e) {
        if (alive.current && !controller.signal.aborted) setError(e.message);
      }
    })();
    return () => {
      alive.current = false;
      controller.abort();
      live.dispose();
    };
  }, [token]);
  const refresh = async () => {
    const version = ++listVersion.current;
    try {
      const options = {
        method: "POST",
        body: "{}",
        signal: requests.current?.signal,
      };
      const results = await Promise.all([
        api("/session/getUserSessionList", token, options),
        api("/session/getGroupSessionList", token, options),
      ]);
      if (!alive.current || version !== listVersion.current) return;
      const list = results.flatMap(r => Array.isArray(r) ? r : r.list || r.sessions || []);
      setSessions(list.filter(s => conversationFromSession(s)));
    } catch (e) {
      if (alive.current && !requests.current?.signal.aborted) setError(e.message);
    }
  };
  const select = (session) => {
    const conversation = conversationFromSession(session);
    if (!conversation) return;
    setCurrent({ ...session, ...conversation, user_id: conversation.receive_id });
    setMobileOpen(false);
    setError("");
    client.current?.select(conversation);
  };
  const open = async (e) => {
    e.preventDefault();
    if (!peer) return;
    try {
      await api("/session/openSession", token, {
        method: "POST",
        body: JSON.stringify({ peer_uuid: peer }),
        signal: requests.current?.signal,
      });
      if (!alive.current) return;
      setPeer("");
      await refresh();
    } catch (e) {
      if (alive.current && !requests.current?.signal.aborted) setError(e.message);
    }
  };
  const send = (e) => {
    e.preventDefault();
    const input = e.currentTarget.elements.content,
      content = input.value.trim();
    if (!content || !current) return;
    try {
      client.current.send(content);
      input.value = "";
    } catch (e) { setError(e.message); }
  };
  const retry = (id) => {
    try { client.current?.retry(id); }
    catch (e) { setError(e.message); }
  };
  const logout = () => {
    alive.current = false;
    requests.current?.abort();
    client.current?.dispose();
    onLogout();
  };
  return (
    <div className="app-shell">
      <aside className={mobileOpen ? "sidebar open" : "sidebar"}>
        <div className="side-head">
          <div>
            <div className="eyebrow">YOUR SPACE</div>
            <strong>{me?.nickname || me?.Nickname || "Fable user"}</strong>
          </div>
          <button
            className="icon-button mobile-only"
            onClick={() => setMobileOpen(false)}
          >
            <X size={19} />
          </button>
        </div>
        <button
          className="new-chat"
          onClick={() => document.querySelector(".peer-input")?.focus()}
        >
          <Plus size={17} /> Start a conversation
        </button>
        <div className="session-head">
          <span>Conversations</span>
          <button
            className="icon-button"
            onClick={refresh}
            aria-label="Refresh"
          >
            <RefreshCw size={16} />
          </button>
        </div>
        <div className="sessions">
          {sessions.map((s, i) => {
            const conversation = conversationFromSession(s), id = conversation.receive_id;
            return (
              <button
                key={conversation.id || i}
                className={
                  current?.id === conversation.id ? "session active" : "session"
                }
                onClick={() => select(s)}
              >
                <span className="avatar">{id?.slice(-2) || "?"}</span>
                <span>
                  <b>{id}</b>
                  <small>{conversation.receive_type === "group" ? "Group conversation" : "Private conversation"}</small>
                </span>
              </button>
            );
          })}
        </div>
        <form className="start-form" onSubmit={open}>
          <input
            className="peer-input"
            placeholder="Peer UUID"
            value={peer}
            onChange={(e) => setPeer(e.target.value)}
          />
          <button className="icon-button" aria-label="Open chat">
            <ArrowLeft size={17} className="flip" />
          </button>
        </form>
        <button className="logout" onClick={logout}>
          <LogOut size={16} /> Log out
        </button>
      </aside>
      <section className="chat-pane">
        <header className="chat-head">
          <button
            className="icon-button mobile-only"
            onClick={() => setMobileOpen(true)}
          >
            <Menu size={21} />
          </button>
          <div className="chat-person">
            <span className="avatar large">
              {current?.user_id?.slice(-2) || <MessageCircle size={19} />}
            </span>
            <div>
              <h2>{current?.user_id || "Select a conversation"}</h2>
              <span>
                {current ? (
                  connected ? (
                    <>
                      <Wifi size={12} /> {syncing ? "Syncing history…" : "Live now"}
                    </>
                  ) : (
                    <>
                      <WifiOff size={12} /> {authRequired ? "Sign in again" : "Connecting…"}
                    </>
                  )
                ) : (
                  "Choose someone from your conversations"
                )}
              </span>
            </div>
          </div>
          {current && <button className="icon-button sync-history" aria-label="Sync history"
            title="Check saved history" onClick={() => client.current?.sync()} disabled={!connected || syncing}>
            <RefreshCw size={16} />
          </button>}
        </header>
        <div className="messages">
          {current ? (
            messages.map((m) => (
              <div
                key={m.message_id || m.client_message_id}
                className={
                  m.send_id === userID
                    ? "message mine"
                    : "message"
                }
              >
                <div className="message-content">{m.content}</div>
                {m.origin === "ai" && (
                  <small>
                    <Sparkles size={12} /> AI assisted
                  </small>
                )}
                {m.status ? <small role="status">
                  {m.status === "pending" ? "Sending…" : "Save unconfirmed"}
                  {m.error && ` · ${m.error.replaceAll("_", " ")}`}
                </small> : m.send_id === userID && <small>
                  {m.delivery === "queued" ? "Saved · queued for push" :
                    m.delivery === "not_queued" ? "Saved · push not queued" :
                      m.delivery === "unknown" ? "Saved · push uncertain" : "Saved"}
                </small>}
                {m.status === "uncertain" && m.retryable && <button type="button" className="retry-message"
                  onClick={() => retry(m.client_message_id)} disabled={!connected}>Retry same request</button>}
              </div>
            ))
          ) : (
            <div className="empty">
              <div className="empty-icon">
                <MessageCircle size={26} />
              </div>
              <h3>Your conversations, in one place.</h3>
              <p>Pick a conversation to begin writing.</p>
            </div>
          )}
        </div>
        {current && (
          <form className="composer" onSubmit={send}>
            <input
              name="content"
              placeholder="Write a message…"
              autoComplete="off"
            />
            <button className="send" aria-label="Send" disabled={!connected || authRequired}>
              <Send size={18} />
            </button>
          </form>
        )}
        {error && (
          <div className="toast" role="alert">
            {error}
            <button onClick={() => setError("")}>×</button>
          </div>
        )}
      </section>
    </div>
  );
}

function App() {
  const [token, setToken] = useState(localStorage.getItem("token") || "");
  return token ? (
    <Chat
      key={token}
      token={token}
      onLogout={() => {
        localStorage.removeItem("token");
        setToken("");
      }}
    />
  ) : (
    <Auth
      onLogin={(t) => {
        localStorage.setItem("token", t);
        setToken(t);
      }}
    />
  );
}
createRoot(document.getElementById("root")).render(<App />);
