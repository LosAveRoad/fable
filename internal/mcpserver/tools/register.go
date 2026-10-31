package tools

import (
	"context"
	"mychat/internal/mcpserver/contract"
	"mychat/internal/mcpserver/prompts"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	ListSessionsName      = "list_sessions"
	GetRecentMessagesName = "get_recent_messages"
	SearchMessagesName    = "search_messages"
	SendMessageName       = "send_message"
)

type SendBackend func(context.Context, string, contract.SendMessageInput) (contract.SendMessageOutput, error)

func Register(server *mcp.Server) { RegisterWithSend(server, nil) }

// RegisterWithSend binds a backend to this server, without a process-global hook.
func RegisterWithSend(server *mcp.Server, backend SendBackend) {
	send := SendMessage
	if backend != nil {
		send = func(ctx context.Context, req *mcp.CallToolRequest, in contract.SendMessageInput) (*mcp.CallToolResult, contract.SendMessageOutput, error) {
			user, err := authenticatedUserUUID(req)
			if err != nil {
				return nil, contract.SendMessageOutput{}, err
			}
			out, err := backend(ctx, user, in)
			if err != nil {
				return nil, contract.SendMessageOutput{}, safeToolError(err)
			}
			return nil, out, nil
		}
	}

	annotations := &mcp.ToolAnnotations{
		ReadOnlyHint:   true,
		IdempotentHint: true,
		OpenWorldHint:  boolPointer(false),
	}
	mcp.AddTool(server, &mcp.Tool{
		Name:        ListSessionsName,
		Title:       "List AI-accessible sessions",
		Description: prompts.ListSessionsDescription,
		Annotations: annotations,
	}, ListSessions)
	mcp.AddTool(server, &mcp.Tool{
		Name:        GetRecentMessagesName,
		Title:       "Get recent session messages",
		Description: prompts.GetRecentMessagesDescription,
		Annotations: annotations,
	}, GetRecentMessages)
	mcp.AddTool(server, &mcp.Tool{
		Name:        SearchMessagesName,
		Title:       "Search authorized messages",
		Description: prompts.SearchMessagesDescription,
		Annotations: annotations,
	}, SearchMessages)
	mcp.AddTool(server, &mcp.Tool{
		Name:        SendMessageName,
		Title:       "Send an authorized session message",
		Description: prompts.SendMessageDescription,
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: boolPointer(false),
			IdempotentHint:  false,
			OpenWorldHint:   boolPointer(false),
		},
	}, send)
}

func boolPointer(value bool) *bool {
	return &value
}
