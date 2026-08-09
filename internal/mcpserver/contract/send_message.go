package contract

type SendMessageInput struct {
	SessionUUID string `json:"session_uuid" jsonschema:"Public UUID of the AI-authorized session in which to send the message."`
	Content     string `json:"content" jsonschema:"Message text to send exactly once, at most 4000 characters."`
}

type SendMessageOutput struct {
	Message Message `json:"message" jsonschema:"The persisted AI-assisted message."`
}
