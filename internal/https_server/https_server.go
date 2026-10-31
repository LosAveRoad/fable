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

// NewLogicEngine retains the legacy business routes and static client, while
// Comet owns WebSocket upgrades. Sync is mounted only by this Logic constructor.
func NewLogicEngine(jwtKey []byte, sync gin.HandlerFunc) *gin.Engine {
	r := gin.Default()
	r.Static("/chat", "./web/chat-server")
	mount(r, jwtKey, LogicRoutes(sync))
	return r
}
