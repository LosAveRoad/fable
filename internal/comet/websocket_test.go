package comet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "mychat/api/gen/fable/v1"

	"github.com/gorilla/websocket"
)

func writerPair(t *testing.T, c *Connection, protocol Protocol, ctx context.Context) (*websocket.Conn, <-chan error) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepted <- conn
	}))
	t.Cleanup(server.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	var peer *websocket.Conn
	select {
	case peer = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("upgrade timed out")
	}
	t.Cleanup(func() { _ = peer.Close() })
	done := make(chan error, 1)
	go func() { done <- c.WriteTo(ctx, peer, protocol) }()
	return client, done
}

func TestSecondWriterCannotConsumeSameConnectionQueue(t *testing.T) {
	h := testHub(t, 2)
	c := register(t, h, testTarget("1"))
	first, done := writerPair(t, c, ProtocolV2, context.Background())
	deliver(t, h, testTarget("1"))
	_ = first.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := first.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	_, secondDone := writerPair(t, c, ProtocolV2, context.Background())
	if err := waitWriter(t, secondDone); !errors.Is(err, ErrWriterAttached) {
		t.Fatalf("second writer = %v", err)
	}
	deliver(t, h, testTarget("1"))
	if _, _, err := first.ReadMessage(); err != nil {
		t.Fatal("first writer affected:", err)
	}
	_, _ = h.Unregister(context.Background(), testTarget("1"))
	if err := waitWriter(t, done); err != nil {
		t.Fatal(err)
	}
}

func waitWriter(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not exit")
		return nil
	}
}

func TestV2WebSocketPreservesLargePositionAndHidesRouteToken(t *testing.T) {
	h := testHub(t, 2)
	c := register(t, h, testTarget("1"))
	peer, done := writerPair(t, c, ProtocolV2, context.Background())
	deliver(t, h, testTarget("1"))
	_ = peer.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, raw, err := peer.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var frame struct {
		Event   string `json:"event"`
		Message struct {
			Position string `json:"position"`
			Origin   string `json:"origin"`
			SendID   string `json:"send_id"`
		} `json:"message"`
	}
	if err = json.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	if frame.Event != "message" || frame.Message.Position != "9007199254740993" || frame.Message.Origin != "user" || frame.Message.SendID != testMessage().SenderId {
		t.Fatalf("wrong public frame: %s", raw)
	}
	if bytes.Contains(raw, []byte("route_token")) || bytes.Contains(raw, []byte(testTarget("1").RouteToken)) {
		t.Fatal("internal identity leaked")
	}
	_, _ = h.Unregister(context.Background(), testTarget("1"))
	if err := waitWriter(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyWebSocketMapsGroupAndAIEnums(t *testing.T) {
	h := testHub(t, 2)
	c := register(t, h, testTarget("1"))
	peer, done := writerPair(t, c, ProtocolLegacy, context.Background())
	m := testMessage()
	m.SessionId = "G60000000-0000-4000-8000-000000000001"
	m.ConversationId = "group:" + m.SessionId
	m.Destination = &pb.Destination{ReceiveType: pb.ReceiveType_RECEIVE_TYPE_GROUP, ReceiveId: m.SessionId}
	m.Origin = pb.MessageOrigin_MESSAGE_ORIGIN_AI
	if _, err := h.Deliver(context.Background(), taskID(), m, []*pb.ConnectionIdentity{testTarget("1")}); err != nil {
		t.Fatal(err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(3 * time.Second))
	var frame map[string]json.RawMessage
	if err := peer.ReadJSON(&frame); err != nil {
		t.Fatal(err)
	}
	if string(frame["receive_type"]) != "1" || string(frame["origin"]) != "1" || frame["event"] != nil {
		t.Fatalf("legacy shape changed: %v", frame)
	}
	_, _ = h.Unregister(context.Background(), testTarget("1"))
	if err := waitWriter(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestWriterCancellationClosesSocketAndUnregisters(t *testing.T) {
	h := testHub(t, 2)
	c := register(t, h, testTarget("1"))
	ctx, cancel := context.WithCancel(context.Background())
	peer, done := writerPair(t, c, ProtocolV2, ctx)
	cancel()
	if err := waitWriter(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("writer error = %v", err)
	}
	waitClosed(t, c)
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := peer.ReadMessage(); err == nil {
		t.Fatal("socket stayed open")
	}
	if deliver(t, h, testTarget("1"))[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_NOT_FOUND {
		t.Fatal("cancelled writer stayed registered")
	}
}

func TestReplacementClosesOldWriterWithoutRemovingNewConnection(t *testing.T) {
	h := testHub(t, 2)
	old := register(t, h, testTarget("1"))
	peer, done := writerPair(t, old, ProtocolV2, context.Background())
	newID := testTarget("1")
	newID.ConnectionId = "20000000-0000-4000-8000-000000000099"
	register(t, h, newID)
	if err := waitWriter(t, done); err != nil {
		t.Fatal(err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := peer.ReadMessage(); err == nil {
		t.Fatal("old socket stayed open")
	}
	if deliver(t, h, newID)[0].Outcome != pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED {
		t.Fatal("old writer unregistered new connection")
	}
}

// Preserve the real HTTP upgrade, then block at net.Conn.Write to make slow
// transport cancellation deterministic without depending on OS buffer sizes.
type blockedWriteConn struct {
	net.Conn
	blocking  atomic.Bool
	entered   chan struct{}
	released  chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
}

func (c *blockedWriteConn) Write(b []byte) (int, error) {
	if c.blocking.Load() {
		c.enterOnce.Do(func() { close(c.entered) })
		<-c.released
		return 0, net.ErrClosed
	}
	return c.Conn.Write(b)
}

func (c *blockedWriteConn) Close() error {
	c.closeOnce.Do(func() { close(c.released) })
	return c.Conn.Close()
}

type blockingListener struct{ net.Listener }

func (l blockingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &blockedWriteConn{Conn: c, entered: make(chan struct{}), released: make(chan struct{})}, nil
}

func TestBlockedSocketWriteCanBeInterrupted(t *testing.T) {
	for _, action := range []string{"cancel", "kick", "drain", "hub-close"} {
		t.Run(action, func(t *testing.T) {
			h := testHub(t, 2)
			c := register(t, h, testTarget("1"))
			accepted := make(chan *websocket.Conn, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				peer, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err == nil {
					accepted <- peer
				}
			}))
			server.Listener = blockingListener{server.Listener}
			server.Start()
			defer server.Close()
			client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			var peer *websocket.Conn
			select {
			case peer = <-accepted:
			case <-time.After(3 * time.Second):
				t.Fatal("upgrade timed out")
			}
			defer peer.Close()
			transport := peer.UnderlyingConn().(*blockedWriteConn)
			transport.blocking.Store(true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- c.WriteTo(ctx, peer, ProtocolV2) }()
			deliver(t, h, testTarget("1"))
			select {
			case <-transport.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("write never blocked")
			}
			switch action {
			case "cancel":
				cancel()
			case "kick":
				_, err = h.Unregister(context.Background(), testTarget("1"))
			case "drain":
				_, err = h.Drain(context.Background(), time.Now().Add(20*time.Millisecond))
			case "hub-close":
				h.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			err = waitWriter(t, done)
			if action == "cancel" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			waitClosed(t, c)
		})
	}
}
