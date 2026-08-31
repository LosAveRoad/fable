package redisservice

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
	"mychat/internal/dao"
)

func TestJSONCacheRoundTripAndTTL(t *testing.T) {
	s := miniredis.RunT(t)
	old := dao.RedisClient
	dao.RedisClient = redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = dao.RedisClient.Close(); dao.RedisClient = old })

	type payload struct {
		Name string `json:"name"`
	}
	ctx := context.Background()
	if err := SetJSON(ctx, "test:key", payload{Name: "group"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	var got payload
	if err := GetJSON(ctx, "test:key", &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "group" {
		t.Fatalf("payload = %+v", got)
	}
	if ttl, err := dao.RedisClient.TTL(ctx, "test:key").Result(); err != nil || ttl <= 0 {
		t.Fatalf("ttl = %v, err = %v", ttl, err)
	}
}

func TestScanDeleteUsesPattern(t *testing.T) {
	s := miniredis.RunT(t)
	old := dao.RedisClient
	dao.RedisClient = redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = dao.RedisClient.Close(); dao.RedisClient = old })
	ctx := context.Background()
	if err := SetEX(ctx, "fable:v1:a", "1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := SetEX(ctx, "other:b", "2", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := ScanDelete(ctx, "fable:v1:*"); err != nil {
		t.Fatal(err)
	}
	if exists, _ := Exists(ctx, "fable:v1:a"); exists {
		t.Fatal("pattern key was not deleted")
	}
	if exists, _ := Exists(ctx, "other:b"); !exists {
		t.Fatal("unmatched key was deleted")
	}
}

func TestOperationsReportUnavailableWithoutClient(t *testing.T) {
	old := dao.RedisClient
	dao.RedisClient = nil
	t.Cleanup(func() { dao.RedisClient = old })
	if _, err := Get(context.Background(), "missing"); err != ErrUnavailable {
		t.Fatalf("error = %v, want %v", err, ErrUnavailable)
	}
}
