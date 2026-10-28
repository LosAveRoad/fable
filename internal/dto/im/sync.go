package im

// SyncQuery represents GET query parameters, not a JSON request body.
// Authentication is middleware state and must not be taken from a user_id param.
type SyncQuery struct {
	ConversationID string `form:"conversation_id"`
	Cursor         string `form:"cursor"`
	Limit          int    `form:"limit"`
}

type SyncPage struct {
	ConversationID string    `json:"conversation_id"`
	Messages       []Message `json:"messages"`
	NextCursor     string    `json:"next_cursor"`
	HasMore        bool      `json:"has_more"`
}
