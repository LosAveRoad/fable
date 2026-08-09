package tools

import (
	"mychat/internal/mcpserver/prompts"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	ListSessionsName      = "list_sessions"
	GetRecentMessagesName = "get_recent_messages"
	SearchMessagesName    = "search_messages"
)

func Register(server *mcp.Server) {
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
}

func boolPointer(value bool) *bool {
	return &value
}
