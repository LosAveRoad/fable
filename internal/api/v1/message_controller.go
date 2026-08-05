package v1

import (
	"mychat/internal/dto/request"
	"mychat/internal/service/gormservice"
	"net/http"

	"github.com/gin-gonic/gin"
)

func GetMessageList(c *gin.Context) {
	var req request.GetMessageListRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid message list request"})
		return
	}

	value, exists := c.Get("user_uuid")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user uuid not found"})
		return
	}
	userUUID, ok := value.(string)
	if !ok || userUUID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user uuid"})
		return
	}

	if userUUID != req.UserOneID && userUUID != req.UserTwoID {
		c.JSON(http.StatusForbidden, gin.H{"error": "user is not a participant"})
		return
	}

	messages, err := gormservice.GetMessageList(req.UserOneID, req.UserTwoID)
	if err != nil {
		if err == gormservice.ErrInvalidUUID {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, messages)
}
