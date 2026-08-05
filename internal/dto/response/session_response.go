package response

type OpenSessionResponse struct {
	SessionUUID string `json:"session_uuid"`
}

type UserSessionListResponse struct {
	SessionUUID string `json:"session_uuid"`
	PeerUUID    string `json:"peer_uuid"`
}
