package aiservice

import "errors"

var (
	ErrInvalidSetting   = errors.New("invalid ai setting")
	ErrInvalidToolInput = errors.New("invalid tool input")
	ErrForbidden        = errors.New("ai session access forbidden")
	ErrMessageNotFound  = errors.New("message not found")
	ErrDatabase         = errors.New("database error")
)
