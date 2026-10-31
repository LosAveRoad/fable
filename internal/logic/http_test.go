package logic

import (
	"context"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang-jwt/jwt/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"mychat/internal/dto/im"
	"mychat/internal/logic/messagestore"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type mcpBearerTransport struct{ token string }

func (b mcpBearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}

func TestMCPSenderLookupDistinguishesMissingUserFromDatabaseFailure(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		failure    bool
	}{{"missing user", "session is not available to AI", false}, {"database failure", "temporarily unavailable", true}} {
		t.Run(tc.name, func(t *testing.T) {
			raw, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			db, err := gorm.Open(mysql.New(mysql.Config{Conn: raw, SkipInitializeWithVersion: true}), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Default.LogMode(logger.Silent)})
			if err != nil {
				t.Fatal(err)
			}
			query := mock.ExpectQuery("SELECT .* FROM `user_info`")
			if tc.failure {
				query.WillReturnError(errors.New("private database details"))
			} else {
				query.WillReturnRows(sqlmock.NewRows([]string{"uuid", "nickname"}))
			}
			key := []byte("logic-test-jwt-key-01234567890123456789")
			token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user_uuid": "U-authenticated", "exp": time.Now().Add(time.Hour).Unix()}).SignedString(key)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(NewHandler(&Service{jwtKey: key}, db, nil))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			client := mcp.NewClient(&mcp.Implementation{Name: "sender-errors", Version: "1"}, nil)
			session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: server.URL + "/mcp", HTTPClient: &http.Client{Transport: mcpBearerTransport{token}}, DisableStandaloneSSE: true}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			out, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "send_message", Arguments: map[string]any{"session_uuid": "S-test", "content": "hello"}})
			if err != nil {
				t.Fatal(err)
			}
			text := ""
			for _, content := range out.Content {
				if c, ok := content.(*mcp.TextContent); ok {
					text += c.Text
				}
			}
			if !out.IsError || !strings.Contains(text, tc.want) || strings.Contains(text, "private database") {
				t.Fatalf("unexpected safe error: %+v %q", out, text)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type syncStore struct {
	user   string
	called int
}

func (s *syncStore) Save(context.Context, messagestore.Input) (messagestore.Saved, error) {
	return messagestore.Saved{}, messagestore.ErrForbidden
}
func (s *syncStore) SaveAI(context.Context, string, string, string, string) (messagestore.Saved, error) {
	return messagestore.Saved{}, messagestore.ErrForbidden
}
func (s *syncStore) Sync(_ context.Context, user, conversation, cursor string, limit int) (im.SyncPage, error) {
	s.user = user
	s.called++
	return im.SyncPage{ConversationID: conversation, Messages: []im.Message{}}, nil
}
func TestLogicHTTPSyncAndDependencyReadiness(t *testing.T) {
	key := []byte("logic-test-jwt-key-01234567890123456789")
	store := &syncStore{}
	s := &Service{messages: store, jwtKey: key}
	h := NewHandler(s, nil, func(context.Context) DependencyStatus { return DependencyStatus{Database: true} })
	token, e := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user_uuid": "U-authenticated", "exp": time.Now().Add(time.Hour).Unix()}).SignedString(key)
	if e != nil {
		t.Fatal(e)
	}
	for _, tc := range []struct {
		path, token string
		want        int
	}{{"/healthz", "", 200}, {"/readyz", "", 503}, {"/wss", "", 404}, {"/api/v2/messages/sync?conversation_id=x", "", 401}, {"/api/v2/messages/sync?conversation_id=x&user_id=attacker", token, 200}, {"/api/v2/messages/sync?limit=-1", token, 400}, {"/api/v2/messages/sync?limit=x", token, 400}} {
		r := httptest.NewRequest(http.MethodGet, tc.path, nil)
		if tc.token != "" {
			r.Header.Set("Authorization", "Bearer "+tc.token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s=%d %s", tc.path, w.Code, w.Body.String())
		}
	}
	if store.user != "U-authenticated" || store.called != 1 {
		t.Fatal("trusted query identity or invalid query reached storage")
	}
}
