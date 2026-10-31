//go:build integration

package logic_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"gorm.io/datatypes"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	pb "mychat/api/gen/fable/v1"
	appauth "mychat/internal/auth"
	"mychat/internal/comet"
	"mychat/internal/dto/im"
	"mychat/internal/logic"
	"mychat/internal/logic/messagestore"
	"mychat/internal/logic/routing"
	"mychat/internal/model"
)

const integrationJobToken = "integration-job-token-0123456789abcdef"
const integrationKickToken = "integration-kick-token-0123456789abcdef"

var integrationJWTKey = []byte("integration-user-jwt-key-0123456789abcdef")

// Only the broker boundary is captured: routing, SQL, Logic and both Comets run
// their production implementations and real network transports.
type integrationPublication struct {
	key string
	raw []byte
}
type integrationPublisher struct {
	mu     sync.Mutex
	sent   []integrationPublication
	err    error
	failAt int
}

func (p *integrationPublisher) Publish(_ context.Context, key string, raw []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, integrationPublication{key, append([]byte(nil), raw...)})
	if p.failAt > 0 && len(p.sent) != p.failAt {
		return nil
	}
	return p.err
}
func (p *integrationPublisher) records() []integrationPublication {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]integrationPublication(nil), p.sent...)
}
func (p *integrationPublisher) fail(err error) { p.mu.Lock(); defer p.mu.Unlock(); p.err = err }

type integrationComet struct {
	id             *pb.CometIdentity
	token, address string
	hub            *comet.Hub
	instance       *comet.Instance
}
type integrationFixture struct {
	db        *gorm.DB
	messages  *messagestore.Store
	routes    *routing.Store
	redis     *redis.Client
	prefix    string
	service   *logic.Service
	client    pb.LogicServiceClient
	publisher *integrationPublisher
	comets    [2]*integrationComet
	users     [3]string
	sessions  [2]string
	group     string
}

func integrationNew(t *testing.T) *integrationFixture {
	t.Helper()
	dsn, addr := os.Getenv("FABLE_LOGIC_TEST_MYSQL_DSN"), os.Getenv("FABLE_LOGIC_TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("real backends require FABLE_LOGIC_TEST_MYSQL_DSN and FABLE_LOGIC_TEST_REDIS_ADDR")
	}
	f := &integrationFixture{publisher: &integrationPublisher{}, prefix: "fable-logic-integration:" + uuid.NewString() + ":"}
	cfg, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = ""
	cfg.ParseTime = true
	admin, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	adminSQL, err := admin.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adminSQL.Close() })
	name := "fable_logic_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if err = admin.Exec("CREATE DATABASE `" + name + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin").Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.Exec("DROP DATABASE `" + name + "`").Error; err != nil {
			t.Errorf("drop isolated test DB: %v", err)
		}
	})
	cfg.DBName = name
	f.db, err = gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := f.db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(20)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err = messagestore.InitSchema(context.Background(), f.db); err != nil {
		t.Fatal(err)
	}
	f.messages = messagestore.New(f.db)
	for i := range f.users {
		f.users[i] = "U" + uuid.NewString()
		integrationDB(t, f.db.Create(&model.UserInfo{UUID: f.users[i], Nickname: fmt.Sprint(i), Telephone: fmt.Sprint(i), PasswordHash: "integration-only"}))
	}
	for i := range f.sessions {
		f.sessions[i] = "S" + uuid.NewString()
		integrationDB(t, f.db.Create(&model.Session{UUID: f.sessions[i], SendId: f.users[0], ReceiveId: f.users[i+1]}))
	}
	f.group = "G" + uuid.NewString()
	members, _ := json.Marshal([]string{f.users[2], f.users[0], f.users[1], f.users[0]})
	integrationDB(t, f.db.Create(&model.GroupInfo{UUID: f.group, Name: "integration", OwnerID: f.users[0], Members: datatypes.JSON(members), Admins: datatypes.JSON(`[]`), MemberCount: 3}))
	f.redis = redis.NewClient(&redis.Options{Addr: addr})
	if err = f.redis.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, err := f.redis.Scan(context.Background(), cursor, f.prefix+"*", 100).Result()
			if err != nil {
				t.Errorf("scan own Redis namespace: %v", err)
				break
			}
			if len(keys) > 0 {
				if err = f.redis.Del(context.Background(), keys...).Err(); err != nil {
					t.Errorf("delete own Redis namespace: %v", err)
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		_ = f.redis.Close()
	})
	f.routes, err = routing.New(f.redis, f.prefix, 30*time.Second, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	grants := make([]logic.CometGrant, 0, 2)
	for n := range f.comets {
		c := &integrationComet{id: &pb.CometIdentity{InstanceId: fmt.Sprintf("integration-%d-%s", n, uuid.NewString()), BootId: uuid.NewString()}, token: fmt.Sprintf("integration-comet-%d-token-0123456789abcdef", n)}
		c.hub, err = comet.NewHub(c.id, 32)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.hub.Close)
		rpc, err := comet.NewRPCService(c.hub, comet.RPCCredentials{JobToken: integrationJobToken, LogicToken: integrationKickToken, AdminToken: "integration-admin-token-0123456789abcdef"})
		if err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		c.address = listener.Addr().String()
		server := grpc.NewServer()
		pb.RegisterCometServiceServer(server, rpc)
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(server.Stop)
		f.comets[n] = c
		grants = append(grants, logic.CometGrant{Identity: c.id, RPCAddress: c.address, Token: c.token})
	}
	f.service, err = logic.NewService(context.Background(), logic.Config{Routes: f.routes, Messages: f.messages, Publisher: f.publisher, JWTSecret: integrationJWTKey, JobToken: integrationJobToken, KickToken: integrationKickToken, Comets: grants, UserExists: func(ctx context.Context, id string) (bool, error) {
		var count int64
		err := f.db.WithContext(ctx).Model(&model.UserInfo{}).Where("uuid = ?", id).Count(&count).Error
		return count == 1, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.service.Close)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterLogicServiceServer(server, f.service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	f.client = pb.NewLogicServiceClient(conn)
	for _, c := range f.comets {
		c.instance, err = comet.StartInstance(context.Background(), c.hub, f.client, c.address, c.token, comet.InstanceOptions{RenewInterval: 100 * time.Millisecond, RPCTimeout: time.Second})
		if err != nil {
			t.Fatalf("real Logic RegisterComet: %v", err)
		}
		t.Cleanup(c.instance.Close)
		if !c.instance.Ready() {
			t.Fatal("Comet not ready after real registration")
		}
	}
	return f
}
func integrationDB(t *testing.T, result *gorm.DB) {
	t.Helper()
	if result.Error != nil {
		t.Fatal(result.Error)
	}
}
func integrationContext(token string) context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}
func integrationJWT(t *testing.T, user string, expiry time.Time) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, &appauth.Claims{UserUUID: user, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(expiry)}}).SignedString(integrationJWTKey)
	if err != nil {
		t.Fatal(err)
	}
	return token
}
func (f *integrationFixture) connect(t *testing.T, n, user int) (*pb.RouteLease, *comet.Connection) {
	t.Helper()
	c := f.comets[n]
	reply, err := f.client.Connect(integrationContext(c.token), &pb.ConnectRequest{Comet: c.id, UserJwt: integrationJWT(t, f.users[user], time.Now().Add(time.Hour)), ConnectionId: uuid.NewString(), RouteToken: base64.RawURLEncoding.EncodeToString([]byte(uuid.NewString()))})
	if err != nil {
		t.Fatal(err)
	}
	local, err := c.hub.Register(context.Background(), reply.Route.Identity)
	if err != nil {
		t.Fatal(err)
	}
	return reply.Route, local
}
func (f *integrationFixture) request(route *pb.RouteLease, receive, content string) *pb.SubmitMessageRequest {
	kind := pb.ReceiveType_RECEIVE_TYPE_USER
	if strings.HasPrefix(receive, "G") {
		kind = pb.ReceiveType_RECEIVE_TYPE_GROUP
	}
	return &pb.SubmitMessageRequest{Identity: route.Identity, ClientMessageId: uuid.NewString(), Destination: &pb.Destination{ReceiveType: kind, ReceiveId: receive}, Content: content, RequestId: "integration-request"}
}
func (f *integrationFixture) send(t *testing.T, n int, request *pb.SubmitMessageRequest) *pb.SendResult {
	t.Helper()
	response, err := f.client.SubmitMessage(integrationContext(f.comets[n].token), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Result == nil || !response.Result.Stored || response.Result.Message == nil || response.Result.Message.Position == 0 {
		t.Fatalf("missing persisted send result: %v", response)
	}
	return response.Result
}
func (f *integrationFixture) count(t *testing.T, want int64) {
	t.Helper()
	var count int64
	integrationDB(t, f.db.Table("message").Count(&count))
	if count != want {
		t.Fatalf("persisted rows = %d, want %d", count, want)
	}
}
func integrationSorted(ids ...string) []string {
	result := append([]string(nil), ids...)
	sort.Strings(result)
	return result
}
func integrationTask(t *testing.T, record integrationPublication) im.PushTask {
	t.Helper()
	var task im.PushTask
	if err := json.Unmarshal(record.raw, &task); err != nil {
		t.Fatal(err)
	}
	if task.SchemaVersion != 1 || task.Kind != "message_push" || task.TaskID != "push:v1:"+task.Message.MessageID+":0" || task.ChunkCount != 1 || task.ChunkIndex != 0 || record.key != task.Message.ConversationID || task.Message.Position == "" || task.Targets == nil {
		t.Fatalf("invalid production push payload: %s", record.raw)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(record.raw, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"pending_recipients", "replay_run_id", "message_id", "conversation_id", "created_at"} {
		if _, ok := raw[key]; ok {
			t.Fatalf("unexpected task field %s: %s", key, record.raw)
		}
	}
	var message map[string]json.RawMessage
	if err := json.Unmarshal(raw["message"], &message); err != nil {
		t.Fatal(err)
	}
	var position string
	if err := json.Unmarshal(message["position"], &position); err != nil {
		t.Fatalf("position must be JSON string: %s", record.raw)
	}
	return task
}
