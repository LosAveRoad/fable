package request

type ChangeAISettingRequest struct {
	// A pointer distinguishes a missing/null field from an explicit empty list,
	// which is the valid request for revoking all AI session access.
	AllowedSessionUUIDs *[]string `json:"allowed_session_uuids"`
}
