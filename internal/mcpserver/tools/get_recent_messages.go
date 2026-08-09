package tools

import (
	"context"

	"mychat/internal/mcpserver/contract"
	"mychat/internal/service/aiservice"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func GetRecentMessages(_ context.Context, req *mcp.CallToolRequest, input contract.GetRecentMessagesInput) (*mcp.CallToolResult, contract.GetRecentMessagesOutput, error) {
	userUUID, err := authenticatedUserUUID(req)
	if err != nil {
		return nil, contract.GetRecentMessagesOutput{}, err
	}
	if err := validateSessionUUID(input.SessionUUID); err != nil {
		return nil, contract.GetRecentMessagesOutput{}, err
	}

	page, err := aiservice.GetRecentMessages(userUUID, input.SessionUUID, input.BeforeMessageUUID, input.Limit)
	if err != nil {
		return nil, contract.GetRecentMessagesOutput{}, safeToolError(err)
	}
	return nil, contract.GetRecentMessagesOutput{
		SessionUUID: input.SessionUUID,
		Messages:    mapMessages(page.Messages),
		HasMore:     page.HasMore,
	}, nil
}
