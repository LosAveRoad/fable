// Package blackbox exercises Fable only through its public HTTP, WebSocket and MCP APIs.
// Fixture accounts remain in the selected disposable environment until its owner tears it down.
package blackbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"mychat/internal/dto/im"
)

// Config always requires an explicit target. Plain HTTP is restricted to loopback.
type Config struct {
	BaseURL     string        `json:"base_url"`
	CAFile      string        `json:"-"`
	RunID       string        `json:"run_id"`
	Timeout     time.Duration `json:"timeout"`
	StepTimeout time.Duration `json:"step_timeout"`
}

func (c Config) normalized() (Config, error) {
	if c.Timeout == 0 {
		c.Timeout = 2 * time.Minute
	}
	if c.StepTimeout == 0 {
		c.StepTimeout = 10 * time.Second
	}
	if c.Timeout < time.Second || c.Timeout > 15*time.Minute || c.StepTimeout < 100*time.Millisecond || c.StepTimeout > 30*time.Second {
		return c, errors.New("timeout outside permitted bounds")
	}
	if c.RunID == "" {
		c.RunID = uuid.NewString()
	}
	if len(c.RunID) > 80 {
		return c, errors.New("run ID exceeds 80 characters")
	}
	for _, r := range c.RunID {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return c, errors.New("run ID must be alphanumeric, dash or underscore")
		}
	}
	return c, nil
}

type client struct {
	config    Config
	base      *url.URL
	http      *http.Client
	transport *http.Transport
	dialer    websocket.Dialer
}

func newClient(cfg Config) (*client, error) {
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || u == nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(cfg.BaseURL, "#") || u.Path != "" && u.Path != "/" || u.Opaque != "" {
		return nil, errors.New("base URL must contain only scheme and host")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return nil, errors.New("base URL requires HTTP or HTTPS")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme == "http" && u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, errors.New("plaintext target must be loopback")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CAFile != "" {
		raw, e := os.ReadFile(cfg.CAFile)
		if e != nil {
			return nil, errors.New("cannot read CA file")
		}
		roots, e := x509.SystemCertPool()
		if e != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(raw) {
			return nil, errors.New("CA file contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	transport.Proxy = nil
	u.Path = ""
	cfg.BaseURL = u.String()
	c := &client{config: cfg, base: u, transport: transport, http: &http.Client{Transport: transport, Timeout: cfg.StepTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	c.dialer = websocket.Dialer{HandshakeTimeout: cfg.StepTimeout, TLSClientConfig: tlsConfig}
	return c, nil
}
func (c *client) close() { c.transport.CloseIdleConnections() }
func (c *client) request(ctx context.Context, method, path, token string, in, out any) (int, error) {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return 0, errors.New("cannot encode request")
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base.String()+path, body)
	if err != nil {
		return 0, errors.New("cannot construct request")
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, errors.New("HTTP transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && out != nil {
		if json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out) != nil {
			return resp.StatusCode, errors.New("invalid HTTP response")
		}
	}
	return resp.StatusCode, nil
}
func (c *client) ok(ctx context.Context, method, path, token string, in, out any) error {
	status, err := c.request(ctx, method, path, token, in, out)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("unexpected HTTP status %d", status)
	}
	return nil
}

type identity struct{ id, token, telephone, password, nickname string }

func makeIdentity() (identity, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return identity{}, errors.New("random source failed")
	}
	phone := "1" + string('3'+raw[31]%7)
	for _, b := range raw[:9] {
		phone += string('0' + b%10)
	}
	return identity{telephone: phone, password: hex.EncodeToString(raw[9:]), nickname: "bb" + hex.EncodeToString(raw[:6])}, nil
}
func (c *client) register(ctx context.Context) (identity, error) {
	u, err := makeIdentity()
	if err != nil {
		return u, err
	}
	var registered struct {
		UUID string `json:"uuid"`
	}
	if err = c.ok(ctx, "POST", "/register", "", map[string]string{"telephone": u.telephone, "password": u.password, "nickname": u.nickname}, &registered); err != nil {
		return u, err
	}
	var login struct {
		UUID  string `json:"uuid"`
		Token string `json:"token"`
	}
	if err = c.ok(ctx, "POST", "/login", "", map[string]string{"telephone": u.telephone, "password": u.password}, &login); err != nil {
		return u, err
	}
	if registered.UUID == "" || registered.UUID != login.UUID || login.Token == "" {
		return u, errors.New("registration/login identity mismatch")
	}
	u.id = login.UUID
	u.token = login.Token
	return u, nil
}
func (c *client) open(ctx context.Context, a, b identity) (string, error) {
	var out struct {
		ID string `json:"session_uuid"`
	}
	err := c.ok(ctx, "POST", "/session/openSession", a.token, map[string]string{"peer_uuid": b.id}, &out)
	if err == nil && out.ID == "" {
		err = errors.New("missing session identity")
	}
	return out.ID, err
}
func (c *client) sync(ctx context.Context, u identity, conversation string, limit int) ([]im.Message, error) {
	var all []im.Message
	seen := map[string]bool{}
	cursors := map[string]bool{}
	cursor := ""
	var last int64
	for pages := 0; pages < 1000; pages++ {
		q := url.Values{"conversation_id": {conversation}, "limit": {fmt.Sprint(limit)}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var page im.SyncPage
		if err := c.ok(ctx, "GET", "/api/v2/messages/sync?"+q.Encode(), u.token, nil, &page); err != nil {
			return nil, err
		}
		if page.ConversationID != conversation || len(page.Messages) > limit {
			return nil, errors.New("invalid sync page")
		}
		for _, m := range page.Messages {
			pos, e := strconv.ParseInt(m.Position, 10, 64)
			if e != nil || strconv.FormatInt(pos, 10) != m.Position || pos <= last || m.MessageID == "" || seen[m.MessageID] || m.ConversationID != conversation {
				return nil, errors.New("duplicate or unordered authoritative history")
			}
			last = pos
			seen[m.MessageID] = true
			all = append(all, m)
		}
		if !page.HasMore {
			return all, nil
		}
		if page.NextCursor == "" || cursors[page.NextCursor] || len(page.Messages) == 0 {
			return nil, errors.New("invalid sync cursor")
		}
		cursor = page.NextCursor
		cursors[cursor] = true
	}
	return nil, errors.New("sync page bound exceeded")
}

type observation struct {
	message im.Message
	at      time.Time
	count   int
}
type socket struct {
	conn     *websocket.Conn
	mu       sync.Mutex
	sendMu   sync.Mutex
	results  map[string][]im.SendResultFrame
	messages map[string]observation
	notify   chan struct{}
	done     chan struct{}
	readErr  error
	once     sync.Once
}

func (c *client) connect(ctx context.Context, u identity) (*socket, error) {
	s, _, err := c.dial(ctx, u.token, "v2", c.base.String())
	return s, err
}
func (c *client) dial(ctx context.Context, token, protocol, origin string) (*socket, int, error) {
	u := *c.base
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	u.Path = "/wss"
	u.RawQuery = url.Values{"protocol": {protocol}, "token": {token}}.Encode()
	conn, resp, err := c.dialer.DialContext(ctx, u.String(), http.Header{"Origin": {origin}})
	status := 0
	if resp != nil {
		status = resp.StatusCode
		if resp.Body != nil {
			resp.Body.Close()
		}
	}
	if err != nil {
		return nil, status, errors.New("WebSocket handshake denied or failed")
	}
	s := &socket{conn: conn, results: map[string][]im.SendResultFrame{}, messages: map[string]observation{}, notify: make(chan struct{}), done: make(chan struct{})}
	conn.SetReadLimit(1 << 20)
	go s.read()
	go func() {
		select {
		case <-ctx.Done():
			s.close()
		case <-s.done:
		}
	}()
	return s, status, nil
}
func (s *socket) signal() { close(s.notify); s.notify = make(chan struct{}) }
func (s *socket) read() {
	defer close(s.done)
	for {
		_, raw, err := s.conn.ReadMessage()
		if err != nil {
			return
		}
		var head struct {
			Event im.EventType `json:"event"`
		}
		if json.Unmarshal(raw, &head) != nil {
			s.fail()
			return
		}
		s.mu.Lock()
		switch head.Event {
		case im.EventMessage:
			var f im.MessageFrame
			if json.Unmarshal(raw, &f) != nil || f.Message.MessageID == "" || len(s.messages) >= 100000 {
				s.readErr = errors.New("invalid or excessive message frames")
			} else {
				o := s.messages[f.Message.MessageID]
				if o.count == 0 {
					o = observation{message: f.Message, at: time.Now()}
				}
				o.count++
				s.messages[f.Message.MessageID] = o
			}
		case im.EventSendResult:
			var f im.SendResultFrame
			if json.Unmarshal(raw, &f) != nil || f.ClientMessageID == "" || (f.Result == nil) == (f.Error == nil) {
				s.readErr = errors.New("invalid send result frame")
			} else {
				s.results[f.ClientMessageID] = append(s.results[f.ClientMessageID], f)
			}
		default:
			s.readErr = errors.New("unexpected WebSocket event")
		}
		bad := s.readErr != nil
		s.signal()
		s.mu.Unlock()
		if bad {
			return
		}
	}
}
func (s *socket) fail() {
	s.mu.Lock()
	s.readErr = errors.New("invalid WebSocket JSON")
	s.signal()
	s.mu.Unlock()
}
func (s *socket) close() { s.once.Do(func() { s.conn.Close() }) }
func (s *socket) send(ctx context.Context, f im.SendFrame) (im.SendResultFrame, error) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(10 * time.Second)
	}
	s.conn.SetWriteDeadline(deadline)
	if s.conn.WriteJSON(f) != nil {
		return im.SendResultFrame{}, errors.New("WebSocket write failed")
	}
	for {
		s.mu.Lock()
		if q := s.results[f.ClientMessageID]; len(q) > 0 {
			out := q[0]
			if len(q) == 1 {
				delete(s.results, f.ClientMessageID)
			} else {
				s.results[f.ClientMessageID] = q[1:]
			}
			s.mu.Unlock()
			return out, nil
		}
		err := s.readErr
		n := s.notify
		s.mu.Unlock()
		if err != nil {
			return im.SendResultFrame{}, err
		}
		select {
		case <-ctx.Done():
			return im.SendResultFrame{}, errors.New("receipt timeout or cancellation")
		case <-s.done:
			return im.SendResultFrame{}, errors.New("WebSocket closed before receipt")
		case <-n:
		}
	}
}
func (s *socket) waitMessage(ctx context.Context, id string) (observation, error) {
	for {
		s.mu.Lock()
		o, ok := s.messages[id]
		err := s.readErr
		n := s.notify
		s.mu.Unlock()
		if ok {
			return o, nil
		}
		if err != nil {
			return o, err
		}
		select {
		case <-ctx.Done():
			return o, errors.New("peer push timeout or cancellation")
		case <-s.done:
			return o, errors.New("WebSocket closed before peer push")
		case <-n:
		}
	}
}
func sendFrame(target string, kind im.ReceiveType, content string) im.SendFrame {
	return im.SendFrame{Event: im.EventSendMessage, ClientMessageID: uuid.NewString(), ReceiveID: target, ReceiveType: kind, Content: content}
}
func stored(f im.SendResultFrame) error {
	if f.Error != nil {
		return fmt.Errorf("send rejected: %s", safeCode(f.Error.Code))
	}
	if f.Result == nil || !f.Result.Stored || f.Result.Message.MessageID == "" || f.Result.ClientMessageID != f.ClientMessageID {
		return errors.New("missing authoritative save receipt")
	}
	return nil
}
func safeCode(c im.ErrorCode) string {
	switch c {
	case im.ErrorInvalidArgument, im.ErrorIdempotencyConflict, im.ErrorUnauthenticated, im.ErrorPermissionDenied, im.ErrorSuperseded, im.ErrorLeaseExpired, im.ErrorInstanceNotRegistered, im.ErrorDependencyUnavailable, im.ErrorResourceExhausted, im.ErrorDraining, im.ErrorResultUnknown:
		return string(c)
	}
	return "unrecognized_error"
}
