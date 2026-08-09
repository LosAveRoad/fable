package tools

import (
	"errors"
	"fmt"
	"time"

	"mychat/internal/mcpserver/contract"
	"mychat/internal/service/aiservice"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func authenticatedUserUUID(req *mcp.CallToolRequest) (string, error) {
	if req == nil || req.Extra == nil || req.Extra.TokenInfo == nil || req.Extra.TokenInfo.UserID == "" {
		return "", errors.New("authenticated user is unavailable")
	}
	return req.Extra.TokenInfo.UserID, nil
}

func safeToolError(err error) error {
	switch {
	case errors.Is(err, aiservice.ErrInvalidToolInput):
		return errors.New("invalid tool input")
	case errors.Is(err, aiservice.ErrForbidden):
		return errors.New("session is not available to AI")
	case errors.Is(err, aiservice.ErrMessageNotFound):
		return errors.New("pagination message was not found in the session")
	default:
		return errors.New("the IM service is temporarily unavailable")
	}
}

func mapMessage(message aiservice.AIMessage) contract.Message {
	return contract.Message{
		MessageUUID: message.UUID,
		SessionUUID: message.SessionUUID,
		Sender: contract.UserSummary{
			UUID: message.SenderUUID,
			Name: message.SenderName,
		},
		Type:      message.Type,
		Content:   message.Content,
		CreatedAt: message.CreatedAt.Format(time.RFC3339Nano),
	}
}

func mapMessages(messages []aiservice.AIMessage) []contract.Message {
	result := make([]contract.Message, 0, len(messages))
	for _, message := range messages {
		result = append(result, mapMessage(message))
	}
	return result
}

func validateSessionUUID(sessionUUID string) error {
	if sessionUUID == "" {
		return fmt.Errorf("session_uuid is required")
	}
	return nil
}
