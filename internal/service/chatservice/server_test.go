package chatservice

import (
	"testing"
	"time"

	"mychat/internal/dto/wschat"
)

func startTestServer(t *testing.T) *Server {
	t.Helper()
	server := NewServer(4)
	go server.Start()
	t.Cleanup(server.Close)
	return server
}

func waitForOnlineCount(t *testing.T, server *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if server.OnlineCount() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("online count = %d, want %d", server.OnlineCount(), want)
}

func waitForClosedClient(t *testing.T, client *Client) {
	t.Helper()
	select {
	case <-client.done:
	case <-time.After(time.Second):
		t.Fatal("client was not closed")
	}
}

func TestNewServerInitializesState(t *testing.T) {
	server := NewServer(4)

	if server.clients == nil || server.register == nil || server.unregister == nil || server.routeQueue == nil || server.stopped == nil {
		t.Fatal("NewServer did not initialize state and channels")
	}
}

func TestServerRegisterAndUnregister(t *testing.T) {
	server := startTestServer(t)
	client := NewClient(server, nil, "U-test-user", 4)

	if !server.Register(client) {
		t.Fatal("Register returned false")
	}
	waitForOnlineCount(t, server, 1)

	server.unregisterClient(client)
	waitForOnlineCount(t, server, 0)
}

func TestOldClientCannotUnregisterReplacement(t *testing.T) {
	server := startTestServer(t)
	first := NewClient(server, nil, "U-test-user", 4)
	second := NewClient(server, nil, "U-test-user", 4)

	if !server.Register(first) || !server.Register(second) {
		t.Fatal("Register returned false")
	}
	waitForClosedClient(t, first)
	waitForOnlineCount(t, server, 1)

	server.unregisterClient(first)
	if server.OnlineCount() != 1 {
		t.Fatalf("online count = %d, want 1", server.OnlineCount())
	}
}

func TestSlowClientDoesNotBlockServer(t *testing.T) {
	server := startTestServer(t)
	slow := NewClient(server, nil, "U-slow", 1)
	if !server.Register(slow) {
		t.Fatal("Register returned false")
	}
	waitForOnlineCount(t, server, 1)

	message := wschat.Message{SendID: "U-sender", ReceiveID: "U-slow", Content: "hello"}
	if !server.deliver(slow, message) {
		t.Fatal("first delivery unexpectedly failed")
	}
	if server.deliver(slow, message) {
		t.Fatal("second delivery unexpectedly succeeded for a full queue")
	}

	waitForClosedClient(t, slow)
	waitForOnlineCount(t, server, 0)

	other := NewClient(server, nil, "U-other", 1)
	if !server.Register(other) {
		t.Fatal("server stopped accepting registrations after slow client")
	}
	waitForOnlineCount(t, server, 1)
}

func TestServerRoutesMessageToExplicitUser(t *testing.T) {
	server := startTestServer(t)
	receiver := NewClient(server, nil, "U-receiver", 1)
	if !server.Register(receiver) {
		t.Fatal("Register returned false")
	}
	waitForOnlineCount(t, server, 1)

	message := wschat.Message{SendID: "U-sender", ReceiveID: "U-receiver", Content: "hello"}
	if !server.RouteTo("U-receiver", message) {
		t.Fatal("RouteTo returned false")
	}

	select {
	case received := <-receiver.outbound:
		if received != message {
			t.Fatalf("received message = %+v, want %+v", received, message)
		}
	case <-time.After(time.Second):
		t.Fatal("submitted message was not routed")
	}
}

func TestServerRouteToRejectsMissingTarget(t *testing.T) {
	server := startTestServer(t)
	if server.RouteTo("", wschat.Message{Content: "hello"}) {
		t.Fatal("RouteTo accepted an empty target")
	}
}

func TestServerCloseStopsLoopAndClients(t *testing.T) {
	server := startTestServer(t)
	first := NewClient(server, nil, "U-first", 1)
	second := NewClient(server, nil, "U-second", 1)

	if !server.Register(first) || !server.Register(second) {
		t.Fatal("Register returned false")
	}
	waitForOnlineCount(t, server, 2)

	server.Close()

	waitForClosedClient(t, first)
	waitForClosedClient(t, second)
	if server.OnlineCount() != 0 {
		t.Fatalf("online count = %d, want 0", server.OnlineCount())
	}
	select {
	case <-server.stopped:
	default:
		t.Fatal("server loop did not stop")
	}
}

func TestNewClientHasSingleOutboundQueue(t *testing.T) {
	server := NewServer(4)
	client := NewClient(server, nil, "U-test-user", 4)

	if client.server != server || client.userUUID != "U-test-user" {
		t.Fatal("NewClient did not retain server and user ownership")
	}
	if client.outbound == nil || client.done == nil {
		t.Fatal("NewClient did not initialize lifecycle channels")
	}
}
