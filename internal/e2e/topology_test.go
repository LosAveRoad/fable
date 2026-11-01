//go:build e2e

package e2e

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/datatypes"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"mychat/internal/dto/im"
	"mychat/internal/model"
)

type manifest struct {
	Secrets  map[string]string `json:"secrets"`
	Database struct {
		Name string `json:"name"`
	} `json:"database"`
	Redis struct {
		Prefix string `json:"prefix"`
	} `json:"redis"`
	Proxy struct {
		URL string `json:"url"`
	} `json:"proxy"`
	Comet []struct {
		HTTP   string `json:"http"`
		BootID string `json:"boot_id"`
	} `json:"comet"`
	Kafka struct {
		Topic   string   `json:"topic"`
		DLQ     string   `json:"dlq_topic"`
		Brokers []string `json:"brokers"`
	} `json:"kafka"`
	Processes map[string]struct {
		PID int `json:"pid"`
	} `json:"processes"`
}
type topology struct {
	root, dir      string
	env            []string
	state          manifest
	db             *gorm.DB
	users          [3]string
	session, group string
	t              *testing.T
	clients        []*wsClient
}

func newTopology(t *testing.T) *topology {
	t.Helper()
	dsn, redisAddress, broker := os.Getenv("FABLE_LOGIC_TEST_MYSQL_DSN"), os.Getenv("FABLE_LOGIC_TEST_REDIS_ADDR"), os.Getenv("FABLE_LOGIC_TEST_KAFKA_BROKER")
	if dsn == "" || redisAddress == "" || broker == "" {
		t.Skip("e2e requires isolated real MySQL, Redis and Kafka env variables")
	}
	cfg, e := mysql.ParseDSN(dsn)
	if e != nil {
		t.Fatal(e)
	}
	cwd, _ := os.Getwd()
	f := &topology{root: filepath.Clean(filepath.Join(cwd, "../..")), dir: t.TempDir(), t: t}
	// Empty owned child directory avoids treating the test harness's parent as runtime state.
	f.dir = filepath.Join(f.dir, "runtime")
	mysqlHost, mysqlPort, _ := strings.Cut(cfg.Addr, ":")
	redisHost, redisPort, _ := strings.Cut(redisAddress, ":")
	f.env = append(os.Environ(), "FABLE_LOCAL_RUNTIME_DIR="+f.dir, "FABLE_LOCAL_EXTERNAL=1", "FABLE_LOCAL_PORT_BASE=0", "FABLE_LOCAL_DATABASE=fable_e2e_"+strings.ReplaceAll(uuid.NewString(), "-", ""), "FABLE_MYSQL_HOST="+mysqlHost, "FABLE_MYSQL_PORT="+mysqlPort, "FABLE_MYSQL_USER="+cfg.User, "FABLE_MYSQL_PASSWORD="+cfg.Passwd, "FABLE_REDIS_HOST="+redisHost, "FABLE_REDIS_PORT="+redisPort, "FABLE_KAFKA_BROKERS="+broker)
	t.Cleanup(func() {
		if err := f.command("down"); err != nil {
			t.Errorf("owned topology cleanup failed: %v", err)
		}
		if t.Failed() {
			f.preserveFailureLogs()
		}
		f.cleanupResources(dsn, redisAddress)
	})
	if e := f.command("up", "--skip-web"); e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(filepath.Join(f.dir, "private.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &f.state); e != nil {
		t.Fatal(e)
	}
	cfg.DBName = f.state.Database.Name
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	f.db, e = gorm.Open(gormmysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if e != nil {
		t.Fatal(e)
	}
	sqlDB, _ := f.db.DB()
	t.Cleanup(func() { sqlDB.Close() })
	for i := range f.users {
		f.users[i] = "U" + uuid.NewString()
		f.check(f.db.Create(&model.UserInfo{UUID: f.users[i], Nickname: fmt.Sprintf("e2e-%d", i), Telephone: uuid.NewString()[:24], PasswordHash: "fixture-only"}).Error)
	}
	f.session = "S" + uuid.NewString()
	a, b := f.users[0], f.users[1]
	if a > b {
		a, b = b, a
	}
	f.check(f.db.Create(&model.Session{UUID: f.session, SendId: a, ReceiveId: b}).Error)
	f.group = "G" + uuid.NewString()
	members, _ := json.Marshal(f.users[:])
	f.check(f.db.Create(&model.GroupInfo{UUID: f.group, Name: "e2e-group", OwnerID: f.users[0], Members: datatypes.JSON(members), Admins: datatypes.JSON(`[]`), MemberCount: 3}).Error)
	f.check(f.db.Create(&model.UserAISessionAccess{UserUUID: f.users[0], SessionUUID: f.session}).Error)
	return f
}
func (f *topology) check(e error) {
	f.t.Helper()
	if e != nil {
		f.t.Fatal(e)
	}
}
func (f *topology) command(args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", append([]string{filepath.Join(f.root, "scripts/local/manage.py")}, args...)...)
	cmd.Env = f.env
	cmd.Dir = f.root
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 30 * time.Second
	raw, e := cmd.CombinedOutput()
	if e != nil {
		return fmt.Errorf("local topology command %s failed: %v\n%s", args[0], e, raw)
	}
	return nil
}

func (f *topology) preserveFailureLogs() {
	out := os.Getenv("FABLE_E2E_ARTIFACT_DIR")
	if out == "" {
		return
	}
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return
	}
	destination := filepath.Join(out, "topology-"+filepath.Base(filepath.Dir(f.dir)))
	if os.MkdirAll(destination, 0700) != nil {
		return
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(f.dir, entry.Name()))
		if err == nil {
			_ = os.WriteFile(filepath.Join(destination, entry.Name()), raw, 0600)
		}
	}
}
func (f *topology) cleanupResources(dsn, redisAddress string) {
	if f.state.Database.Name == "" {
		raw, _ := os.ReadFile(filepath.Join(f.dir, "private.json"))
		_ = json.Unmarshal(raw, &f.state)
	}
	if strings.HasPrefix(f.state.Database.Name, "fable_e2e_") {
		cfg, e := mysql.ParseDSN(dsn)
		if e == nil {
			cfg.DBName = ""
			db, e := gorm.Open(gormmysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
			if e == nil {
				db.Exec("DROP DATABASE `" + f.state.Database.Name + "`")
				sqlDB, _ := db.DB()
				sqlDB.Close()
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if f.state.Redis.Prefix != "" {
		r := redis.NewClient(&redis.Options{Addr: redisAddress})
		defer r.Close()
		var cursor uint64
		for {
			keys, next, e := r.Scan(ctx, cursor, f.state.Redis.Prefix+"*", 100).Result()
			if e != nil {
				break
			}
			if len(keys) > 0 {
				r.Del(ctx, keys...)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	if len(f.state.Kafka.Brokers) > 0 {
		transport := &kafka.Transport{}
		defer transport.CloseIdleConnections()
		admin := &kafka.Client{Addr: kafka.TCP(f.state.Kafka.Brokers...), Transport: transport}
		_, _ = admin.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{f.state.Kafka.Topic, f.state.Kafka.DLQ}})
	}
}
func (f *topology) token(user string, expiry time.Time) string {
	v, e := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user_uuid": user, "exp": expiry.Unix()}).SignedString([]byte(f.state.Secrets["jwt"]))
	f.check(e)
	return v
}

type wsClient struct {
	socket   *websocket.Conn
	mu       sync.Mutex
	messages map[string]im.Message
	results  map[string]im.SendResult
	done     chan struct{}
}

func (f *topology) connect(user string, comet int) *wsClient {
	f.t.Helper()
	token := f.token(user, time.Now().Add(time.Hour))
	url := "ws://" + f.state.Comet[comet].HTTP + "/wss?protocol=v2&token=" + token
	socket, _, e := websocket.DefaultDialer.Dial(url, http.Header{"Origin": []string{f.state.Proxy.URL}})
	f.check(e)
	c := &wsClient{socket: socket, messages: map[string]im.Message{}, results: map[string]im.SendResult{}, done: make(chan struct{})}
	f.clients = append(f.clients, c)
	f.t.Cleanup(func() { socket.Close() })
	go func() {
		defer close(c.done)
		for {
			_, raw, e := socket.ReadMessage()
			if e != nil {
				return
			}
			var header struct {
				Event string `json:"event"`
			}
			if json.Unmarshal(raw, &header) != nil {
				continue
			}
			c.mu.Lock()
			switch header.Event {
			case "message":
				var m im.MessageFrame
				if json.Unmarshal(raw, &m) == nil {
					c.messages[m.Message.MessageID] = m.Message
				}
			case "send_result":
				var r im.SendResultFrame
				if json.Unmarshal(raw, &r) == nil && r.Result != nil {
					c.results[r.ClientMessageID] = *r.Result
				}
			}
			c.mu.Unlock()
		}
	}()
	return c
}
func wait(t *testing.T, label string, fn func() bool) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out: " + label)
}
func (f *topology) send(c *wsClient, target string, kind im.ReceiveType, id, content string) im.SendResult {
	f.t.Helper()
	c.mu.Lock()
	delete(c.results, id)
	c.mu.Unlock()
	f.check(c.socket.WriteJSON(im.SendFrame{Event: im.EventSendMessage, ClientMessageID: id, ReceiveID: target, ReceiveType: kind, Content: content}))
	var result im.SendResult
	wait(f.t, "send_result", func() bool { c.mu.Lock(); defer c.mu.Unlock(); var ok bool; result, ok = c.results[id]; return ok })
	if !result.Stored || result.Dispatch != im.DispatchQueued {
		f.t.Fatalf("expected saved queued result: %+v", result)
	}
	return result
}
func seen(t *testing.T, c *wsClient, id string) {
	wait(t, "cross-instance message", func() bool { c.mu.Lock(); defer c.mu.Unlock(); _, ok := c.messages[id]; return ok })
}

func TestRealTwoLogicTwoCometTwoJobChat(t *testing.T) {
	f := newTopology(t)
	run := func(name string, fn func(*testing.T)) {
		t.Run(name, func(child *testing.T) { f.t = child; defer func() { f.t = t }(); fn(child) })
	}
	a, b, c := f.connect(f.users[0], 0), f.connect(f.users[1], 1), f.connect(f.users[2], 1)
	run("100 bidirectional persisted messages", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			sender, peer, target := a, b, f.users[1]
			if i%2 == 1 {
				sender, peer, target = b, a, f.users[0]
			}
			r := f.send(sender, target, im.ReceiveTypeUser, uuid.NewString(), fmt.Sprintf("direct-%03d", i))
			seen(t, peer, r.Message.MessageID)
		}
		var count int64
		f.check(f.db.Table("message").Count(&count).Error)
		if count != 100 {
			t.Fatalf("message rows=%d", count)
		}
	})
	run("three member group and same id replay", func(t *testing.T) {
		id := uuid.NewString()
		r := f.send(a, f.group, im.ReceiveTypeGroup, id, "three users across Comets")
		seen(t, a, r.Message.MessageID)
		seen(t, b, r.Message.MessageID)
		seen(t, c, r.Message.MessageID)
		replay := f.send(a, f.group, im.ReceiveTypeGroup, id, "three users across Comets")
		if !replay.Replayed || replay.Message.MessageID != r.Message.MessageID {
			t.Fatal("retry changed stored message")
		}
		var count int64
		f.check(f.db.Table("message").Count(&count).Error)
		if count != 101 {
			t.Fatalf("replay rows=%d", count)
		}
	})
	run("MCP through ingress publishes AI message", func(t *testing.T) {
		client := mcp.NewClient(&mcp.Implementation{Name: "e2e", Version: "1"}, nil)
		session, e := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: f.state.Proxy.URL + "/mcp", HTTPClient: &http.Client{Transport: bearer{f.token(f.users[0], time.Now().Add(time.Hour))}, Timeout: 10 * time.Second}, DisableStandaloneSSE: true}, nil)
		f.check(e)
		defer session.Close()
		out, e := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "send_message", Arguments: map[string]any{"session_uuid": f.session, "client_message_id": uuid.NewString(), "content": "AI over actual Kafka"}})
		f.check(e)
		if out.IsError {
			t.Fatalf("MCP error: %+v", out)
		}
		raw, _ := json.Marshal(out.StructuredContent)
		var result struct {
			Result *im.SendResult `json:"result"`
		}
		f.check(json.Unmarshal(raw, &result))
		if result.Result == nil || !result.Result.Stored || result.Result.Message.Origin != im.MessageOriginAI {
			t.Fatal("missing AI stored result")
		}
		seen(t, a, result.Result.Message.MessageID)
		seen(t, b, result.Result.Message.MessageID)
	})
	run("consumer process restart recovers queued tasks without new rows", func(t *testing.T) {
		f.check(f.command("stop-jobs"))
		var ids []string
		for i := 0; i < 3; i++ {
			r := f.send(a, f.users[1], im.ReceiveTypeUser, uuid.NewString(), fmt.Sprintf("queued during Job outage %d", i))
			ids = append(ids, r.Message.MessageID)
		}
		b.mu.Lock()
		for _, id := range ids {
			if _, ok := b.messages[id]; ok {
				t.Error("message delivered while both Job processes stopped")
			}
		}
		b.mu.Unlock()
		f.check(f.command("restart-job", "--index", "1"))
		for _, id := range ids {
			seen(t, b, id)
		}
		f.check(f.command("restart-job", "--index", "2"))
		var count int64
		f.check(f.db.Table("message").Count(&count).Error)
		if count != 105 {
			t.Fatalf("consumer restart persisted again: rows=%d", count)
		}
	})
	run("new Comet kicks old connection and offline sync restores", func(t *testing.T) {
		migrated := f.connect(f.users[0], 1)
		select {
		case <-a.done:
		case <-time.After(5 * time.Second):
			t.Fatal("old socket was not kicked")
		}
		r := f.send(migrated, f.users[1], im.ReceiveTypeUser, uuid.NewString(), "after migration")
		seen(t, b, r.Message.MessageID)
		b.socket.Close()
		<-b.done
		offline := f.send(migrated, f.users[1], im.ReceiveTypeUser, uuid.NewString(), "offline authoritative recovery")
		_ = f.connect(f.users[1], 0)
		request, e := http.NewRequest("GET", f.state.Proxy.URL+"/api/v2/messages/sync?conversation_id=direct:"+f.session+"&limit=100", nil)
		f.check(e)
		request.Header.Set("Authorization", "Bearer "+f.token(f.users[1], time.Now().Add(time.Hour)))
		reply, e := http.DefaultClient.Do(request)
		f.check(e)
		defer reply.Body.Close()
		var page im.SyncPage
		f.check(json.NewDecoder(reply.Body).Decode(&page))
		found := false
		for {
			for _, m := range page.Messages {
				if m.MessageID == offline.Message.MessageID {
					found = true
				}
			}
			if !page.HasMore {
				break
			}
			request.URL.RawQuery = "conversation_id=direct:" + f.session + "&limit=100&cursor=" + page.NextCursor
			next, e := http.DefaultClient.Do(request)
			f.check(e)
			f.check(json.NewDecoder(next.Body).Decode(&page))
			next.Body.Close()
		}
		if !found {
			t.Fatal("offline saved message absent from sync")
		}
	})
	run("invalid Kafka bytes are preserved in DLQ", func(t *testing.T) {
		raw := []byte{0xff, '{', 'x'}
		writer := &kafka.Writer{Addr: kafka.TCP(f.state.Kafka.Brokers...), Topic: f.state.Kafka.Topic, RequiredAcks: kafka.RequireAll, MaxAttempts: 1}
		defer writer.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		f.check(writer.WriteMessages(ctx, kafka.Message{Key: []byte("invalid-e2e"), Value: raw}))
		reader := kafka.NewReader(kafka.ReaderConfig{Brokers: f.state.Kafka.Brokers, Topic: f.state.Kafka.DLQ, GroupID: "fable-e2e-dlq-" + uuid.NewString(), StartOffset: kafka.FirstOffset, MaxWait: 100 * time.Millisecond})
		defer reader.Close()
		message, e := reader.FetchMessage(ctx)
		f.check(e)
		var record struct {
			RawPayloadBase64 string `json:"raw_payload_base64"`
		}
		f.check(json.Unmarshal(message.Value, &record))
		if record.RawPayloadBase64 != base64.StdEncoding.EncodeToString(raw) {
			t.Fatal("DLQ lost exact invalid bytes")
		}
		endOffsets := func() int64 {
			transport := &kafka.Transport{}
			defer transport.CloseIdleConnections()
			admin := &kafka.Client{Addr: kafka.TCP(f.state.Kafka.Brokers...), Transport: transport}
			requests := make([]kafka.OffsetRequest, 6)
			for i := range requests {
				requests[i] = kafka.LastOffsetOf(i)
			}
			r, e := admin.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{f.state.Kafka.Topic: requests}})
			f.check(e)
			var total int64
			for _, p := range r.Topics[f.state.Kafka.Topic] {
				f.check(p.Error)
				total += p.LastOffset
			}
			return total
		}
		before := endOffsets()
		args := []string{"replay-dlq", "-run-id", "e2e-invalid-preview", "-partition", strconv.Itoa(message.Partition), "-from-offset", strconv.FormatInt(message.Offset, 10), "-to-offset", strconv.FormatInt(message.Offset, 10)}
		f.check(f.command(args...))
		f.check(f.command(append(args, "-execute")...))
		if after := endOffsets(); after != before {
			t.Fatalf("invalid DLQ record was republished: %d→%d", before, after)
		}
	})
	if e := f.command("status"); e != nil {
		t.Fatal(e)
	}
	if e := f.command("up", "--skip-web"); e != nil {
		t.Fatal("idempotent up:", e)
	}
	run("route state loss closes old leases and full restart preserves messages", func(t *testing.T) {
		beforeBoot := f.state.Comet[0].BootID
		r := redis.NewClient(&redis.Options{Addr: os.Getenv("FABLE_LOGIC_TEST_REDIS_ADDR")})
		defer r.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var cursor uint64
		for {
			keys, next, e := r.Scan(ctx, cursor, f.state.Redis.Prefix+"*", 100).Result()
			f.check(e)
			if len(keys) > 0 {
				f.check(r.Del(ctx, keys...).Err())
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		deadline := time.After(25 * time.Second)
		for _, client := range f.clients {
			select {
			case <-client.done:
			case <-deadline:
				t.Fatal("connection survived lost instance/route state")
			}
		}
		f.check(f.command("down"))
		f.check(f.command("up", "--skip-web"))
		raw, e := os.ReadFile(filepath.Join(f.dir, "private.json"))
		f.check(e)
		f.check(json.Unmarshal(raw, &f.state))
		if f.state.Comet[0].BootID == beforeBoot {
			t.Fatal("full restart reused Comet boot identity")
		}
		var count int64
		f.check(f.db.Table("message").Count(&count).Error)
		if count != 107 {
			t.Fatalf("restart changed authoritative rows: %d", count)
		}
		left, right := f.connect(f.users[0], 0), f.connect(f.users[1], 1)
		message := f.send(left, f.users[1], im.ReceiveTypeUser, uuid.NewString(), "after route loss and full restart")
		seen(t, right, message.Message.MessageID)
	})
}

type bearer struct{ token string }

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}
