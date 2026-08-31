package gormservice

import "errors"

var (
	// 注册
	ErrInvalidRegister  = errors.New("invalid register request")
	ErrTelephoneExists  = errors.New("telephone already exists")
	ErrUsernameExists   = errors.New("username already exists")
	ErrCreateUserFailed = errors.New("create user failed")

	// 登录
	ErrUserNotFound    = errors.New("user not found")
	ErrInvalidPassword = errors.New("invalid password")
	ErrLoginFailed     = errors.New("login failed")

	// 获取用户信息
	ErrUserAccessDenied = errors.New("user access denied")
	ErrInvalidUUID      = errors.New("invalid uuid")
	ErrForbidden        = errors.New("forbidden")

	// 数据库异常
	ErrDatabase              = errors.New("database error")
	ErrInvalidUserPair       = errors.New("invalid message user pair")
	ErrInvalidMessageContent = errors.New("invalid message content")

	// 会话
	ErrInvalidSession        = errors.New("invalid session request")
	ErrSessionCreateFail     = errors.New("create session failed")
	ErrInvalidGroup          = errors.New("invalid group request")
	ErrGroupNotFound         = errors.New("group not found")
	ErrGroupUnavailable      = errors.New("group unavailable")
	ErrGroupJoinForbidden    = errors.New("group join forbidden")
	ErrGroupOwnerCannotLeave = errors.New("group owner cannot leave")
	ErrNotGroupMember        = errors.New("user is not a group member")
	ErrGroupJoinPending      = errors.New("group join request pending")
	ErrNotGroupAdmin         = errors.New("user is not a group administrator")
)
