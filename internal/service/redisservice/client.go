package redisservice

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	redis "github.com/redis/go-redis/v9"
	"mychat/internal/dao"
)

var ErrUnavailable = errors.New("redis unavailable")

func client() (*redis.Client, error) {
	if dao.RedisClient == nil {
		return nil, ErrUnavailable
	}
	return dao.RedisClient, nil
}

func Get(ctx context.Context, key string) (string, error) {
	c, err := client()
	if err != nil {
		return "", err
	}
	return c.Get(ctx, key).Result()
}

func SetEX(ctx context.Context, key, value string, ttl time.Duration) error {
	c, err := client()
	if err != nil {
		return err
	}
	return c.Set(ctx, key, value, ttl).Err()
}

func Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	c, err := client()
	if err != nil {
		return err
	}
	return c.Del(ctx, keys...).Err()
}

func Exists(ctx context.Context, key string) (bool, error) {
	c, err := client()
	if err != nil {
		return false, err
	}
	n, err := c.Exists(ctx, key).Result()
	return n > 0, err
}

func ScanDelete(ctx context.Context, pattern string) error {
	c, err := client()
	if err != nil {
		return err
	}
	var cursor uint64
	for {
		keys, next, err := c.Scan(ctx, cursor, pattern, 256).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := c.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func GetJSON(ctx context.Context, key string, target any) error {
	value, err := Get(ctx, key)
	if err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(value), target); err != nil {
		_ = Delete(ctx, key)
		return err
	}
	return nil
}

func SetJSON(ctx context.Context, key string, value any, ttl time.Duration) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return SetEX(ctx, key, string(raw), ttl)
}

func IsNil(err error) bool { return errors.Is(err, redis.Nil) }
