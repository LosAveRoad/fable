package v1

import (
	"mychat/internal/dto/request"
	"mychat/internal/service/gormservice"
	"net/http"

	"github.com/gin-gonic/gin"
)

func OpenSession(c *gin.Context) {
	var req request.OpenSessionRequest

	if err := c.ShouldBindBodyWithJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, err)
		return
	}

	SendUUID, ok := c.Get("user_uuid")
	if !ok {
		c.JSON(http.StatusForbidden, "you haven't loggin")
		return
	}

	sendUUID, ok := SendUUID.(string)
	if !ok {
		c.JSON(http.StatusInternalServerError, "invalid user uuid")
		return
	}

	response, err := gormservice.OpenSession(sendUUID, req.PeerUUID)
	if err != nil {
		switch err {
		case gormservice.ErrInvalidSession:
			c.JSON(http.StatusBadRequest, err)
		case gormservice.ErrUserNotFound:
			c.JSON(http.StatusNotFound, err)
		default:
			c.JSON(http.StatusInternalServerError, err)
		}
		return
	}

	c.JSON(http.StatusOK, response)
}

// GetUserSessionList 处理 POST /session/getUserSessionList。

func GetUserSessionList(c *gin.Context) {
	value, exists := c.Get("user_uuid")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user uuid not found"})
		return
	}

	userUUID, ok := value.(string)
	if !ok || userUUID == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid user uuid"})
		return
	}

	response, err := gormservice.GetUserSessionList(userUUID)
	if err != nil {
		if err == gormservice.ErrInvalidUUID {
			c.JSON(http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, response)
}
