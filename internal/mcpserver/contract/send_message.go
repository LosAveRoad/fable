package contract

import "mychat/internal/dto/im"

type SendMessageInput struct {
	ClientMessageID string `json:"client_message_id,omitempty" jsonschema:"Optional UUID reused for retries of the same sender and content."`
	SessionUUID     string `json:"session_uuid" jsonschema:"Public UUID of the AI-authorized session in which to send the message."`
	Content         string `json:"content" jsonschema:"Message text to send exactly once, at most 4000 characters."`
}

type SendMessageOutput struct {
	Result  *im.SendResult `json:"result,omitempty" jsonschema:"Logic storage and dispatch result; online push is best effort."`
	Message Message        `json:"message" jsonschema:"The persisted AI-assisted message."`
}
