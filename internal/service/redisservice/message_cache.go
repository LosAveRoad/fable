package redisservice

import (
	"context"
	"time"
)

const DefaultCacheTTL = 10 * time.Minute

func AppendJSON(ctx context.Context, key string, item any) error {
	var items []any
	if err := GetJSON(ctx, key, &items); err != nil {
		if IsNil(err) || err == ErrUnavailable {
			return nil
		}
		return err
	}
	items = append(items, item)
	return SetJSON(ctx, key, items, DefaultCacheTTL)
}
