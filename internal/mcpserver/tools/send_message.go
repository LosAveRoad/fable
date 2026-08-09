package tools

import (
	"context"

	"mychat/internal/dto/wschat"
	"mychat/internal/mcpserver/contract"
	"mychat/internal/service/aiservice"
	"mychat/internal/service/chatservice"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func SendMessage(_ context.Context, req *mcp.CallToolRequest, input contract.SendMessageInput) (*mcp.CallToolResult, contract.SendMessageOutput, error) {
	userUUID, err := authenticatedUserUUID(req)
	if err != nil {
		return nil, contract.SendMessageOutput{}, err
	}
	if err := validateSessionUUID(input.SessionUUID); err != nil {
		return nil, contract.SendMessageOutput{}, err
	}

	created, err := aiservice.SendMessage(userUUID, input.SessionUUID, input.Content)
	if err != nil {
		return nil, contract.SendMessageOutput{}, safeToolError(err)
	}

	realtimeMessage := wschat.Message{
		SendID:    created.SenderUUID,
		ReceiveID: created.ReceiverUUID,
		Content:   created.Content,
		Origin:    created.Origin,
	}
	chatservice.ChatServer.RouteTo(created.ReceiverUUID, realtimeMessage)
	chatservice.ChatServer.RouteTo(created.SenderUUID, realtimeMessage)

	return nil, contract.SendMessageOutput{Message: mapMessage(created)}, nil
}
