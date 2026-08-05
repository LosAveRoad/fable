package request

type OpenSessionRequest struct {
	PeerUUID string `json:"peer_uuid" binding:"required"`
}

// GetUserSessionListRequest 不包含字段，当前用户身份来自 JWT。
type GetUserSessionListRequest struct{}
