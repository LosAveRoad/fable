//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"mychat/internal/config"
	"mychat/internal/dao"
	"mychat/internal/dto/response"
	"mychat/internal/https_server"
	"mychat/internal/mcpserver"
	"mychat/internal/model"
	"mychat/internal/service/chatservice"
	"mychat/internal/service/gormservice"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

var (
	testJWTKey = []byte("mychat-integration-test-secret")
	testServer *httptest.Server
)

type testUser struct {
	UUID  string
	Token string
}

func TestMain(m *testing.M) {
	if err := dao.InitGorm(testMySQLConfig()); err != nil {
		fmt.Fprintf(os.Stderr, "integration test database is unavailable: %v\n", err)
		os.Exit(1)
	}
	gormservice.InitJWT(config.JWTConfig{Secret: testJWTKey})

	cleanupDatabase()
	chatservice.ChatServer = chatservice.NewServer(chatservice.DefaultQueueSize)
	go chatservice.ChatServer.Start()
	root := http.NewServeMux()
	root.Handle("/mcp", mcpserver.NewHTTPHandler(mcpserver.New(), testJWTKey))
	root.Handle("/", https_server.NewEngine(testJWTKey))
	testServer = httptest.NewServer(root)

	code := m.Run()

	testServer.Close()
	chatservice.ChatServer.Close()
	cleanupDatabase()
	_ = dao.CloseGorm()
	os.Exit(code)
}

func testMySQLConfig() config.MySQLConfig {
	return config.MySQLConfig{
		Host:         envOrDefault("TEST_MYSQL_HOST", "127.0.0.1"),
		Port:         3306,
		User:         envOrDefault("TEST_MYSQL_USER", "root"),
		Password:     envOrDefault("TEST_MYSQL_PASSWORD", "mychat-dev-password"),
		DatabaseName: envOrDefault("TEST_MYSQL_DATABASE", "mychat_test"),
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func cleanupDatabase() {
	if dao.GormDB == nil {
		return
	}
	_ = dao.GormDB.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&model.UserAISessionAccess{}).Error
	_ = dao.GormDB.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&model.Message{}).Error
	_ = dao.GormDB.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&model.Session{}).Error
	_ = dao.GormDB.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&model.UserInfo{}).Error
}

func requestJSON(t *testing.T, method string, path string, body any, token string, result any) *http.Response {
	t.Helper()

	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, testServer.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			t.Fatalf("decode %s response: %v", path, err)
		}
	}
	return resp
}

func postJSON(t *testing.T, path string, body any, token string, result any) *http.Response {
	t.Helper()

	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPost, testServer.URL+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	if result != nil {
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			t.Fatalf("decode %s response: %v", path, err)
		}
	}
	return resp
}

func createTestUser(t *testing.T) testUser {
	t.Helper()

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	phone := fmt.Sprintf("139%08d", time.Now().UnixNano()%100000000)
	nickname := "integration-" + suffix

	registerResponse := postJSON(t, "/register", map[string]string{
		"telephone": phone,
		"password":  "test123456",
		"nickname":  nickname,
	}, "", &response.RegisterResponse{})
	if registerResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("register status = %d, want %d", registerResponse.StatusCode, http.StatusAccepted)
	}
	registerResponse.Body.Close()

	var login response.LoginResponse
	loginResponse := postJSON(t, "/login", map[string]string{
		"telephone": phone,
		"password":  "test123456",
	}, "", &login)
	if loginResponse.StatusCode != http.StatusOK {
		loginResponse.Body.Close()
		t.Fatalf("login status = %d, want %d", loginResponse.StatusCode, http.StatusOK)
	}
	loginResponse.Body.Close()

	return testUser{UUID: login.UUID, Token: login.Token}
}

func openSession(t *testing.T, user testUser, peer testUser) {
	t.Helper()

	resp := postJSON(t, "/session/openSession", map[string]string{
		"peer_uuid": peer.UUID,
	}, user.Token, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("open session status = %d, body = %s", resp.StatusCode, body)
	}
}

func websocketURL(t *testing.T, user testUser) string {
	t.Helper()

	parsed, err := url.Parse(testServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Scheme = "ws"
	parsed.Path = "/wss"
	query := parsed.Query()
	query.Set("token", user.Token)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func waitForMessage(t *testing.T, conn interface {
	SetReadDeadline(time.Time) error
	ReadJSON(any) error
}) wschatMessage {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var message wschatMessage
	if err := conn.ReadJSON(&message); err != nil {
		t.Fatal(err)
	}
	return message
}

type wschatMessage struct {
	SendID    string `json:"send_id"`
	ReceiveID string `json:"receive_id"`
	Content   string `json:"content"`
}
