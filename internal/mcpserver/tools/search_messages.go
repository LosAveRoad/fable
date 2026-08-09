package tools

import (
	"context"

	"mychat/internal/mcpserver/contract"
	"mychat/internal/service/aiservice"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func SearchMessages(_ context.Context, req *mcp.CallToolRequest, input contract.SearchMessagesInput) (*mcp.CallToolResult, contract.SearchMessagesOutput, error) {
	userUUID, err := authenticatedUserUUID(req)
	if err != nil {
		return nil, contract.SearchMessagesOutput{}, err
	}
	messages, err := aiservice.SearchMessages(userUUID, input.Query, input.SessionUUID, input.Limit)
	if err != nil {
		return nil, contract.SearchMessagesOutput{}, safeToolError(err)
	}
	return nil, contract.SearchMessagesOutput{Messages: mapMessages(messages)}, nil
}
