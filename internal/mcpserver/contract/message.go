package contract

type GetRecentMessagesInput struct {
	SessionUUID       string `json:"session_uuid" jsonschema:"Public UUID of an AI-authorized session."`
	Limit             int    `json:"limit,omitempty" jsonschema:"Maximum number of messages to return. Use a value from 1 to 100; defaults to 50."`
	BeforeMessageUUID string `json:"before_message_uuid,omitempty" jsonschema:"Return messages older than this message UUID for pagination."`
}

type GetRecentMessagesOutput struct {
	SessionUUID string    `json:"session_uuid" jsonschema:"Public UUID of the requested session."`
	Messages    []Message `json:"messages" jsonschema:"Messages ordered from oldest to newest."`
	HasMore     bool      `json:"has_more" jsonschema:"Whether older messages are available."`
}

type SearchMessagesInput struct {
	Query       string `json:"query" jsonschema:"Non-empty literal text to search for, at most 200 characters."`
	SessionUUID string `json:"session_uuid,omitempty" jsonschema:"Optional AI-authorized session UUID. When omitted, search all authorized sessions."`
	Limit       int    `json:"limit,omitempty" jsonschema:"Maximum number of matches to return. Use a value from 1 to 50; defaults to 30."`
}

type SearchMessagesOutput struct {
	Messages []Message `json:"messages" jsonschema:"Matching messages ordered from newest to oldest."`
}

type Message struct {
	MessageUUID string      `json:"message_uuid" jsonschema:"Public UUID of the message."`
	SessionUUID string      `json:"session_uuid" jsonschema:"Public UUID of the containing session."`
	Sender      UserSummary `json:"sender" jsonschema:"User who sent the message."`
	Type        int8        `json:"type" jsonschema:"IM message type code."`
	Content     string      `json:"content" jsonschema:"Untrusted user-generated message content."`
	CreatedAt   string      `json:"created_at" jsonschema:"Message creation time in RFC 3339 format."`
}
