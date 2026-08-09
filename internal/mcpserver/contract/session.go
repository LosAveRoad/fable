package contract

type ListSessionsInput struct {
	Limit int `json:"limit,omitempty" jsonschema:"Maximum number of sessions to return. Use a value from 1 to 100; defaults to 50."`
}

type ListSessionsOutput struct {
	Sessions []Session `json:"sessions" jsonschema:"Sessions the authenticated user explicitly allows AI to access."`
}

type Session struct {
	SessionUUID string      `json:"session_uuid" jsonschema:"Public UUID of the session."`
	Peer        UserSummary `json:"peer" jsonschema:"The other participant in this direct-message session."`
	CreatedAt   string      `json:"created_at" jsonschema:"Session creation time in RFC 3339 format."`
}

type UserSummary struct {
	UUID string `json:"uuid" jsonschema:"Public UUID of the user."`
	Name string `json:"name" jsonschema:"Display name of the user."`
}
