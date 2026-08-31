package dao

import (
	"context"
	"errors"
	"fmt"

	redis "github.com/redis/go-redis/v9"
	"mychat/internal/config"
)

var RedisClient *redis.Client

// InitRedis creates the process-wide Redis client and verifies connectivity.
// Redis is deliberately initialized separately from MySQL so callers can
// choose whether a Redis outage is fatal through RedisConfig.Required.
func InitRedis(cfg config.RedisConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if RedisClient != nil {
		return errors.New("redis has already been initialized")
	}
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		return fmt.Errorf("ping redis: %w", err)
	}
	RedisClient = client
	return nil
}

func CloseRedis() error {
	if RedisClient == nil {
		return nil
	}
	err := RedisClient.Close()
	RedisClient = nil
	return err
}
