package response

type AISettingResponse struct {
	Sessions []AISettingSessionResponse `json:"sessions"`
}

type AISettingSessionResponse struct {
	SessionUUID     string                `json:"session_uuid"`
	Peer            AISettingPeerResponse `json:"peer"`
	AIAccessAllowed bool                  `json:"ai_access_allowed"`
}

type AISettingPeerResponse struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}
