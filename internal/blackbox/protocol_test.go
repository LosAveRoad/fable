package blackbox

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"mychat/internal/dto/im"
)

func TestSocketReceivesPushBeforeReceiptAndCancels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, e := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer conn.Close()
		var f im.SendFrame
		if conn.ReadJSON(&f) != nil {
			return
		}
		m := im.Message{MessageID: "M1", Content: f.Content}
		conn.WriteJSON(im.MessageFrame{Event: im.EventMessage, Message: m})
		conn.WriteJSON(im.SendResultFrame{Event: im.EventSendResult, ClientMessageID: f.ClientMessageID, Result: &im.SendResult{ClientMessageID: f.ClientMessageID, Stored: true, Message: m}})
		conn.ReadMessage()
	}))
	defer server.Close()
	c, e := newClient(Config{BaseURL: server.URL})
	if e != nil {
		t.Fatal(e)
	}
	defer c.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, e := c.connect(ctx, identity{token: "test"})
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	step, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	f, e := s.send(step, sendFrame("U2", im.ReceiveTypeUser, "test"))
	if e != nil || stored(f) != nil {
		t.Fatalf("receipt: %v", e)
	}
	o, e := s.waitMessage(step, "M1")
	if e != nil || o.count != 1 || o.message.Content != "test" {
		t.Fatalf("early push lost: %v", e)
	}
	cancel()
	select {
	case <-s.done:
	case <-time.After(time.Second):
		t.Fatal("reader leaked after cancellation")
	}
}
func TestMalformedReceiptIsFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, e := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer conn.Close()
		var f im.SendFrame
		if conn.ReadJSON(&f) != nil {
			return
		}
		conn.WriteJSON(map[string]any{"event": "send_result", "client_message_id": f.ClientMessageID, "result": map[string]any{}, "error": map[string]any{"code": "secret-server-error"}})
	}))
	defer server.Close()
	c, _ := newClient(Config{BaseURL: server.URL})
	defer c.close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, e := c.connect(ctx, identity{})
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	_, e = s.send(ctx, sendFrame("U2", im.ReceiveTypeUser, "test"))
	if e == nil || strings.Contains(e.Error(), "secret-server-error") {
		t.Fatalf("invalid response handling: %v", e)
	}
}

// driverServer is a protocol peer for accounting tests, not an application fixture bypass.
// Every driver operation still goes through HTTP or WebSocket on this test server.
type driverServer struct {
	mu                    sync.Mutex
	users                 map[string]string
	sockets               map[string]*websocket.Conn
	messages              []im.Message
	dropLive, omitHistory bool
}

func (d *driverServer) serve(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/register":
		var in map[string]string
		json.NewDecoder(r.Body).Decode(&in)
		d.mu.Lock()
		id := fmt.Sprintf("U%d", len(d.users)+1)
		d.users[in["telephone"]] = id
		d.mu.Unlock()
		w.WriteHeader(201)
		json.NewEncoder(w).Encode(map[string]string{"uuid": id})
	case "/login":
		var in map[string]string
		json.NewDecoder(r.Body).Decode(&in)
		d.mu.Lock()
		id := d.users[in["telephone"]]
		d.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]string{"uuid": id, "token": id})
	case "/session/openSession":
		json.NewEncoder(w).Encode(map[string]string{"session_uuid": "S1"})
	case "/wss":
		conn, e := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer conn.Close()
		user := r.URL.Query().Get("token")
		d.mu.Lock()
		d.sockets[user] = conn
		d.mu.Unlock()
		for {
			var f im.SendFrame
			if conn.ReadJSON(&f) != nil {
				return
			}
			d.mu.Lock()
			m := im.Message{MessageID: fmt.Sprintf("M%d", len(d.messages)+1), Position: fmt.Sprint(len(d.messages) + 1), ConversationID: "direct:S1", Content: f.Content, SendID: user, ReceiveID: f.ReceiveID}
			d.messages = append(d.messages, m)
			peer := d.sockets[f.ReceiveID]
			if peer != nil && (!d.dropLive || !strings.HasPrefix(f.Content, "blackbox-load-")) {
				peer.WriteJSON(im.MessageFrame{Event: im.EventMessage, Message: m})
			}
			conn.WriteJSON(im.SendResultFrame{Event: im.EventSendResult, ClientMessageID: f.ClientMessageID, Result: &im.SendResult{ClientMessageID: f.ClientMessageID, Stored: true, Message: m, Dispatch: im.DispatchQueued}})
			d.mu.Unlock()
		}
	case "/api/v2/messages/sync":
		d.mu.Lock()
		messages := append([]im.Message(nil), d.messages...)
		d.mu.Unlock()
		if d.omitHistory {
			messages = nil
		}
		json.NewEncoder(w).Encode(im.SyncPage{ConversationID: "direct:S1", Messages: messages})
	default:
		w.WriteHeader(404)
	}
}
func TestLoadSeparatesOnlineAndAuthoritativeRecovery(t *testing.T) {
	for _, tc := range []struct {
		name          string
		drop, missing bool
		minRatio      float64
		wantPass      bool
	}{{"online", false, false, 1, true}, {"sync recovery", true, false, 0, true}, {"live threshold", true, false, 1, false}, {"missing saved", false, true, 0, false}} {
		t.Run(tc.name, func(t *testing.T) {
			d := &driverServer{users: map[string]string{}, sockets: map[string]*websocket.Conn{}, dropLive: tc.drop, omitHistory: tc.missing}
			server := httptest.NewServer(http.HandlerFunc(d.serve))
			defer server.Close()
			report, e := RunLoad(context.Background(), LoadConfig{Config: Config{BaseURL: server.URL, StepTimeout: time.Second}, Pairs: 1, Rate: 4, Duration: time.Second, ReceiveTimeout: 100 * time.Millisecond, MinReceiveRatio: tc.minRatio})
			if report.Passed != tc.wantPass || (e == nil) != tc.wantPass {
				t.Fatalf("pass=%v err=%v report=%+v", report.Passed, e, report)
			}
			if report.Attempted < 2 || report.Attempted != report.Stored {
				t.Fatalf("bad stored accounting: %+v", report)
			}
			if tc.missing {
				if report.Missing != report.Stored {
					t.Fatal("missing stored messages hidden")
				}
				return
			}
			if tc.drop {
				if report.Received != 0 || report.Recovered != report.Stored || report.ReceiveLatency.Samples != 0 {
					t.Fatalf("sync falsely counted as live: %+v", report)
				}
			} else if report.Received != report.Stored || report.Recovered != 0 || report.ReceiveLatency.Samples != report.Stored {
				t.Fatalf("live measurements lost: %+v", report)
			}
		})
	}
}
