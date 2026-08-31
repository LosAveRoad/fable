package redisservice

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
	"mychat/internal/dao"
	"testing"
)

func TestAuthCodeConsume(t *testing.T) {
	s := miniredis.RunT(t)
	old := dao.RedisClient
	dao.RedisClient = redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = dao.RedisClient.Close(); dao.RedisClient = old })
	ctx := context.Background()
	if err := SetAuthCode(ctx, "13900000000", "1234"); err != nil {
		t.Fatal(err)
	}
	if ok, err := ConsumeAuthCode(ctx, "13900000000", "bad"); err != nil || ok {
		t.Fatalf("wrong code ok=%v err=%v", ok, err)
	}
	if ok, err := ConsumeAuthCode(ctx, "13900000000", "1234"); err != nil || !ok {
		t.Fatalf("consume ok=%v err=%v", ok, err)
	}
	if _, err := GetAuthCode(ctx, "13900000000"); !IsNil(err) {
		t.Fatalf("code was not deleted: %v", err)
	}
}
