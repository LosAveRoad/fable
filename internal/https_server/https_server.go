package https_server

import (
	"github.com/gin-gonic/gin"
	v1 "mychat/internal/api/v1"
)

func NewEngine(jwtKey []byte) *gin.Engine {
	r := gin.Default()
	r.Static("/chat", "./web/chat-server")
	mount(r, jwtKey, legacyRoutes())
	r.GET("/wss", WsAuth(jwtKey), v1.WsController)
	return r
}

// NewLogicEngine retains business routes while Comet owns WebSocket upgrades.
// The local ingress serves the v2 client; old assets stay at the legacy entry.
func NewLogicEngine(jwtKey []byte, sync gin.HandlerFunc) *gin.Engine {
	r := gin.Default()
	mount(r, jwtKey, LogicRoutes(sync))
	return r
}
