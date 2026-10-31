package httpdocs_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"mychat/internal/dao"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	appauth "mychat/internal/auth"
	"mychat/internal/dto/im"
	"mychat/internal/httpdocs"
	server "mychat/internal/https_server"
)

func document(t *testing.T) map[string]any {
	t.Helper()
	b, err := httpdocs.Generate(server.LogicRoutes(func(c *gin.Context) {}))
	if err != nil {
		t.Fatal(err)
	}
	var d map[string]any
	if err = json.Unmarshal(b, &d); err != nil {
		t.Fatal(err)
	}
	return d
}
func TestDocumentCoversActualLogicRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	d := document(t)
	if d["openapi"] != "3.1.0" {
		t.Fatal("must be actual OpenAPI 3.1")
	}
	paths := d["paths"].(map[string]any)
	engine := server.NewLogicEngine([]byte("key"), func(c *gin.Context) {})
	count := 0
	for _, route := range engine.Routes() {
		if strings.HasPrefix(route.Path, "/chat/") {
			continue
		}
		count++
		p, ok := paths[route.Path].(map[string]any)
		if !ok || p[strings.ToLower(route.Method)] == nil {
			t.Fatalf("undocumented route %s %s", route.Method, route.Path)
		}
	}
	documented := 0
	for p, v := range paths {
		if p == "/wss" {
			t.Fatal("WebSocket advertised as REST")
		}
		documented += len(v.(map[string]any))
	}
	if count != documented || count != 19 {
		t.Fatalf("actual=%d documented=%d; want 19", count, documented)
	}
	legacy := server.NewEngine([]byte("key"))
	hasWS := false
	for _, r := range legacy.Routes() {
		if r.Path == "/wss" {
			hasWS = true
		}
		if r.Path == "/api/v2/messages/sync" {
			t.Fatal("legacy engine gained sync")
		}
	}
	if !hasWS {
		t.Fatal("legacy engine lost WebSocket")
	}
}
func token(t *testing.T) string {
	t.Helper()
	s, e := jwt.NewWithClaims(jwt.SigningMethodHS256, appauth.Claims{UserUUID: "Uone", RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}}).SignedString([]byte("key"))
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func TestProtectedRoutesRejectMissingJWT(t *testing.T) {
	engine := server.NewLogicEngine([]byte("key"), func(c *gin.Context) { t.Error("sync called without JWT") })
	for _, route := range server.LogicRoutes(func(c *gin.Context) {}) {
		if !route.Authenticated {
			continue
		}
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest(route.Method, route.Path, nil))
		if w.Code != 401 || w.Body.String() != `{"error":"missing authorization header"}` {
			t.Fatalf("%s: %d %s", route.Path, w.Code, w.Body.String())
		}
	}
}
func TestSyncPassesJWTIdentityAndQuery(t *testing.T) {
	engine := server.NewLogicEngine([]byte("key"), func(c *gin.Context) {
		if c.GetString("user_uuid") != "Uone" || c.Query("cursor") != "opaque" {
			t.Error("identity/query missing")
		}
		c.JSON(200, im.SyncPage{ConversationID: "direct:example", Messages: []im.Message{}, NextCursor: "opaque"})
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/v2/messages/sync?cursor=opaque", nil)
	r.Header.Set("Authorization", "Bearer "+token(t))
	engine.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"messages":[]`) {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}
func TestLegacyHistoryErrorContract(t *testing.T) {
	engine := server.NewLogicEngine([]byte("key"), func(c *gin.Context) {})
	for _, tc := range []struct {
		body    string
		status  int
		message string
	}{
		{`{}`, 400, "invalid message list request"},
		{`{"user_one_id":"Uother","user_two_id":"Uelse"}`, 403, "user is not a participant"},
		{`{"user_one_id":"Uone","user_two_id":"Uone"}`, 400, "invalid uuid"},
	} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/message/getMessageList", strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer "+token(t))
		r.Header.Set("Content-Type", "application/json")
		engine.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
		var e struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil || e.Error == "" {
			t.Fatalf("unexpected error body %s", w.Body.String())
		}
		if tc.status == 403 && e.Error != tc.message {
			t.Fatalf("%s", e.Error)
		}
	}
}
func TestSchemaReflectsOptionalAndRequiredJSONFields(t *testing.T) {
	type example struct {
		Required string  `json:"required" binding:"required"`
		Optional string  `json:"optional,omitempty"`
		Hidden   string  `json:"-"`
		Nullable *string `json:"nullable"`
	}
	s := httpdocs.Schema(example{}, false)
	p := s["properties"].(map[string]any)
	if p["Hidden"] != nil || p["-"] != nil {
		t.Fatal("json:- leaked")
	}
	required := s["required"].([]string)
	if strings.Join(required, ",") != "required,nullable" {
		t.Fatalf("required=%v", required)
	}
	request := httpdocs.Schema(example{}, true)
	if strings.Join(request["required"].([]string), ",") != "required" {
		t.Fatal("request required must follow binding tags")
	}
}
func TestHistoryAndSyncSchemaShapes(t *testing.T) {
	d := document(t)
	paths := d["paths"].(map[string]any)
	op := paths["/message/getMessageList"].(map[string]any)["post"].(map[string]any)
	responses := op["responses"].(map[string]any)
	schema := func(status string) map[string]any {
		return responses[status].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)
	}
	if schema("200")["type"] != "array" {
		t.Fatal("history must be a bare array")
	}
	if schema("400")["properties"].(map[string]any)["error"] == nil {
		t.Fatal("legacy error shape lost")
	}
	sync := paths["/api/v2/messages/sync"].(map[string]any)["get"].(map[string]any)
	if sync["requestBody"] != nil {
		t.Fatal("sync is query, not body")
	}
	if len(sync["parameters"].([]any)) != 3 {
		t.Fatal("missing sync query parameters")
	}
	if len(sync["security"].([]any)) != 1 {
		t.Fatal("sync requires JWT")
	}
}

func TestLegacyHistorySuccessIsBareArrayAndDatabaseErrorIsObject(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	gdb, err := gorm.Open(mysql.New(mysql.Config{Conn: db, SkipInitializeWithVersion: true}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	oldDB, oldRedis := dao.GormDB, dao.RedisClient
	dao.GormDB = gdb
	dao.RedisClient = nil
	defer func() { dao.GormDB = oldDB; dao.RedisClient = oldRedis }()
	query := regexp.QuoteMeta("SELECT * FROM `message` WHERE (send_id = ? AND receive_id = ?) OR (send_id = ? AND receive_id = ?) ORDER BY created_at ASC, id ASC")
	mock.ExpectQuery(query).WithArgs("Uone", "Utwo", "Utwo", "Uone").WillReturnRows(sqlmock.NewRows([]string{"uuid", "session_id", "content", "send_id", "receive_id", "created_at"}).AddRow("Mexample", "Sexample", "hello", "Uone", "Utwo", time.Date(2026, 10, 26, 1, 2, 3, 0, time.UTC)))
	mock.ExpectQuery(query).WithArgs("Uone", "Utwo", "Utwo", "Uone").WillReturnRows(sqlmock.NewRows([]string{"uuid"}))
	mock.ExpectQuery(query).WithArgs("Uone", "Utwo", "Utwo", "Uone").WillReturnError(errors.New("query failed"))
	engine := server.NewLogicEngine([]byte("key"), func(c *gin.Context) {})
	for i, want := range []int{200, 200, 500} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/message/getMessageList", strings.NewReader(`{"user_one_id":"Uone","user_two_id":"Utwo"}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+token(t))
		engine.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		if i == 0 {
			var rows []map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil || len(rows) != 1 || rows[0]["content"] != "hello" || rows[0]["created_at"] != "2026-10-26T01:02:03Z" {
				t.Fatalf("history body=%s err=%v", w.Body.String(), err)
			}
		}
		if i == 1 && w.Body.String() != "[]" {
			t.Fatalf("empty history body=%s", w.Body.String())
		}
		if i == 2 {
			var e map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil || e["error"] == "" {
				t.Fatalf("error body=%s", w.Body.String())
			}
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestCommittedDocumentIsCurrent(t *testing.T) {
	got, err := httpdocs.Generate(server.LogicRoutes(func(c *gin.Context) {}))
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("../../api/openapi/http.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("OpenAPI is stale: run go run ./cmd/httpdocs")
	}
}
func TestGenerateRejectsDuplicateRoute(t *testing.T) {
	routes := server.LogicRoutes(func(c *gin.Context) {})
	duplicate := routes[0]
	duplicate.ID = "duplicate"
	routes = append(routes, duplicate)
	if _, err := httpdocs.Generate(routes); err == nil {
		t.Fatal("duplicate method/path accepted")
	}
}

func TestPublicValidationKeepsHistoricalErrors(t *testing.T) {
	engine := server.NewLogicEngine([]byte("key"), func(c *gin.Context) {})
	for _, tc := range []struct{ path, body, want string }{
		{"/register", `{}`, `{"error":"Invalid Register Request"}`},
		{"/login", `{}`, `[{},{}]`},
	} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body))
		r.Header.Set("Content-Type", "application/json")
		engine.ServeHTTP(w, r)
		if w.Code != 400 || w.Body.String() != tc.want {
			t.Fatalf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
	}
}
func TestControllerRequiredAIFieldIsNonNullable(t *testing.T) {
	d := document(t)
	paths := d["paths"].(map[string]any)
	op := paths["/api/v1/ai/setting"].(map[string]any)["put"].(map[string]any)
	schema := op["requestBody"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)
	required := schema["required"].([]any)
	if len(required) != 1 || required[0] != "allowed_session_uuids" {
		t.Fatal(required)
	}
	field := schema["properties"].(map[string]any)["allowed_session_uuids"].(map[string]any)
	if field["type"] != "array" || field["anyOf"] != nil {
		t.Fatal("controller rejects null and missing AI allow list")
	}
	login := paths["/login"].(map[string]any)["post"].(map[string]any)["responses"].(map[string]any)["200"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)
	if login["properties"].(map[string]any)["nickname"] != nil {
		t.Fatal("json hidden login nickname documented")
	}
}
