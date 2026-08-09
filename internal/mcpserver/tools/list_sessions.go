package tools

import (
	"context"
	"time"

	"mychat/internal/mcpserver/contract"
	"mychat/internal/service/aiservice"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func ListSessions(_ context.Context, req *mcp.CallToolRequest, input contract.ListSessionsInput) (*mcp.CallToolResult, contract.ListSessionsOutput, error) {
	userUUID, err := authenticatedUserUUID(req)
	if err != nil {
		return nil, contract.ListSessionsOutput{}, err
	}
	sessions, err := aiservice.ListAllowedSessions(userUUID, input.Limit)
	if err != nil {
		return nil, contract.ListSessionsOutput{}, safeToolError(err)
	}

	output := contract.ListSessionsOutput{Sessions: make([]contract.Session, 0, len(sessions))}
	for _, session := range sessions {
		output.Sessions = append(output.Sessions, contract.Session{
			SessionUUID: session.UUID,
			Peer: contract.UserSummary{
				UUID: session.PeerUUID,
				Name: session.PeerName,
			},
			CreatedAt: session.CreatedAt.Format(time.RFC3339Nano),
		})
	}
	return nil, output, nil
}
