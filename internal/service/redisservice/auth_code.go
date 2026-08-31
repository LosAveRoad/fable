package redisservice

import (
	"context"
	"time"
)

const AuthCodeTTL = time.Minute

func SetAuthCode(ctx context.Context, telephone, code string) error {
	return SetEX(ctx, AuthCodeKey(telephone), code, AuthCodeTTL)
}
func GetAuthCode(ctx context.Context, telephone string) (string, error) {
	return Get(ctx, AuthCodeKey(telephone))
}
func ConsumeAuthCode(ctx context.Context, telephone, expected string) (bool, error) {
	actual, err := GetAuthCode(ctx, telephone)
	if err != nil {
		if IsNil(err) {
			return false, nil
		}
		return false, err
	}
	if actual != expected {
		return false, nil
	}
	if err := Delete(ctx, AuthCodeKey(telephone)); err != nil {
		return false, err
	}
	return true, nil
}
