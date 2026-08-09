package mcpserver

import (
	"net/http"

	"mychat/internal/mcpserver/prompts"
	"mychat/internal/mcpserver/tools"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func New() *mcp.Server {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "fable-im", Version: "v0.1.0"},
		&mcp.ServerOptions{
			Instructions: prompts.ServerInstructions,
			Capabilities: &mcp.ServerCapabilities{},
		},
	)
	tools.Register(server)
	return server
}

func NewHTTPHandler(server *mcp.Server, jwtSecret []byte) http.Handler {
	handler := mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return server },
		nil,
	)
	return requireBearerToken(jwtSecret)(handler)
}
