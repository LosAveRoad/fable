package chatservice

import "testing"

func TestRegisterUserCreatesUserChannel(t *testing.T) {
	mu.Lock()
	previousUsers := onlineUsers
	onlineUsers = make(map[string]*Client)
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		onlineUsers = previousUsers
		mu.Unlock()
	})

	RegisterUser("U-test-user")

	mu.Lock()
	client, ok := onlineUsers["U-test-user"]
	mu.Unlock()

	if !ok {
		t.Fatal("registered user was not stored")
	}
	if client == nil {
		t.Fatal("registered client is nil")
	}
	if client.userUUID != "U-test-user" {
		t.Fatalf("userUUID = %q, want %q", client.userUUID, "U-test-user")
	}
	if client.ch == nil {
		t.Fatal("registered client channel is nil")
	}
}

func TestRegisterUserReplacesExistingClient(t *testing.T) {
	mu.Lock()
	previousUsers := onlineUsers
	onlineUsers = make(map[string]*Client)
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		onlineUsers = previousUsers
		mu.Unlock()
	})

	RegisterUser("U-test-user")
	mu.Lock()
	firstClient := onlineUsers["U-test-user"]
	mu.Unlock()

	RegisterUser("U-test-user")
	mu.Lock()
	secondClient := onlineUsers["U-test-user"]
	mu.Unlock()

	if firstClient == secondClient {
		t.Fatal("registering the same user did not replace the client")
	}
}
