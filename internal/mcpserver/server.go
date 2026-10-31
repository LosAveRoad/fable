package mcpserver

import (
	"net/http"

	"mychat/internal/mcpserver/prompts"
	"mychat/internal/mcpserver/tools"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func New() *mcp.Server { return NewWithSend(nil) }

func NewWithSend(send tools.SendBackend) *mcp.Server {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "fable-im", Version: "v0.1.0"},
		&mcp.ServerOptions{
			Instructions: prompts.ServerInstructions,
			Capabilities: &mcp.ServerCapabilities{},
		},
	)
	tools.RegisterWithSend(server, send)
	return server
}

func NewHTTPHandler(server *mcp.Server, jwtSecret []byte) http.Handler {
	return newHTTPHandler(server, jwtSecret, false)
}

// NewStatelessHTTPHandler supports independent Logic replicas. Fable's tools
// authenticate each request and keep durable state in SQL, so they need no
// process-local MCP session or sticky ingress routing.
func NewStatelessHTTPHandler(server *mcp.Server, jwtSecret []byte) http.Handler {
	return newHTTPHandler(server, jwtSecret, true)
}

func newHTTPHandler(server *mcp.Server, jwtSecret []byte, stateless bool) http.Handler {
	handler := mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{Stateless: stateless},
	)
	return requireBearerToken(jwtSecret)(handler)
}
