package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"mychat/internal/dao"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	l.Close()
	return address
}
func realConfig(t *testing.T) Config {
	t.Helper()
	dsn := os.Getenv("FABLE_LOGIC_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("FABLE_LOGIC_TEST_MYSQL_DSN required for real isolated database tests")
	}
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid test DSN")
	}
	parsed.DBName = ""
	admin, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		t.Fatal("open test administrator")
	}
	name := "fable_logic_test_app_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec("CREATE DATABASE `" + name + "`"); err != nil {
		t.Fatal("cannot create isolated test database")
	}
	t.Cleanup(func() { admin.Exec("DROP DATABASE `" + name + "`"); admin.Close() })
	c := validConfig()
	host, port, err := net.SplitHostPort(parsed.Addr)
	if err != nil {
		t.Fatal("test DSN requires TCP")
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal("test port invalid")
	}
	c.Base.MySQLConfig.Host = host
	c.Base.MySQLConfig.Port = n
	c.Base.MySQLConfig.User = parsed.User
	c.Base.MySQLConfig.Password = parsed.Passwd
	c.Base.MySQLConfig.DatabaseName = name
	c.HTTPAddress = freeAddress(t)
	c.RPCAddress = freeAddress(t)
	return c
}
func TestRealRuntimeDegradedStartupAndCancellation(t *testing.T) {
	c := realConfig(t)
	ctx := context.Background()
	if err := InitSchema(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := InitSchema(ctx, c); err == nil {
		t.Fatal("second schema init must refuse nonempty DB")
	}
	// Deliberately unavailable route/push dependencies must not stop HTTP/history.
	_, redisPort, _ := net.SplitHostPort(freeAddress(t))
	c.Base.RedisConfig.Port, _ = strconv.Atoi(redisPort)
	c.Base.KafkaConfig.Brokers = []string{freeAddress(t)}
	oldRedis := dao.RedisClient
	legacy := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	dao.RedisClient = legacy
	defer func() { dao.RedisClient = oldRedis; legacy.Close() }()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(runCtx, c) }()
	client := &http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for {
		r, err := client.Get("http://" + c.HTTPAddress + "/healthz")
		if err == nil {
			r.Body.Close()
			if r.StatusCode != 200 {
				t.Fatalf("health status %d", r.StatusCode)
			}
			break
		}
		select {
		case err := <-done:
			t.Fatalf("runtime stopped before health: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("runtime did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
	r, err := client.Get("http://" + c.HTTPAddress + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]bool
	err = json.NewDecoder(r.Body).Decode(&state)
	r.Body.Close()
	if err != nil || r.StatusCode != 503 || !state["database"] || state["routes"] || state["push"] {
		t.Fatalf("degraded ready status=%d state=%v err=%v", r.StatusCode, state, err)
	}
	r, err = client.Get("http://" + c.HTTPAddress + "/wss")
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != 404 {
		t.Fatal("Logic exposed WebSocket")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Run shutdown exceeded bound")
	}
	if dao.GormDB != nil || dao.RedisClient != legacy {
		t.Fatal("global database/cache state not restored")
	}
	for _, address := range []string{c.HTTPAddress, c.RPCAddress} {
		if conn, err := net.DialTimeout("tcp", address, 50*time.Millisecond); err == nil {
			conn.Close()
			t.Fatal("listener remained open")
		}
	}
}
func TestRealServeRefusesLegacySchemaWithoutMigration(t *testing.T) {
	c := realConfig(t)
	if err := InitSchema(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	db, raw, err := openDatabase(context.Background(), c.Base.MySQLConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if err := db.Exec("ALTER TABLE message DROP COLUMN recipient_ids").Error; err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), c); err == nil {
		t.Fatal("serve accepted incomplete target schema")
	}
	var count int
	err = db.Raw("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'message' AND column_name = 'recipient_ids'").Scan(&count).Error
	if err != nil || count != 0 {
		t.Fatal("serve migrated the selected database")
	}
}
