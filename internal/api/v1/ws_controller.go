package v1

import (
	"mychat/internal/service/chatservice"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func WsController(c *gin.Context) {
	conn, err := upgrader.Upgrade(
		c.Writer,
		c.Request,
		nil,
	)
	if err != nil {
		return
	}

	userID, ok := c.Get("user_id")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "unauthorized",
		})
	}

	uid := userID.(string)

	chatservice.RegisterUser(uid)

	go chatservice.ReadPump(conn, uid)
	go chatservice.WritePump(conn, uid)
}
