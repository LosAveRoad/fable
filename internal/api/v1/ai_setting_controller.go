package v1

import (
	"errors"
	"net/http"

	"mychat/internal/dto/request"
	"mychat/internal/service/aiservice"

	"github.com/gin-gonic/gin"
)

func GetAISetting(c *gin.Context) {
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

	setting, err := aiservice.GetAISetting(userUUID)
	if err != nil {
		writeAISettingError(c, err)
		return
	}
	c.JSON(http.StatusOK, setting)
}

func ChangeAISetting(c *gin.Context) {
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

	var req request.ChangeAISettingRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.AllowedSessionUUIDs == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ai setting request"})
		return
	}

	setting, err := aiservice.ChangeAISetting(userUUID, *req.AllowedSessionUUIDs)
	if err != nil {
		writeAISettingError(c, err)
		return
	}
	c.JSON(http.StatusOK, setting)
}

func writeAISettingError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, aiservice.ErrInvalidSetting):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case errors.Is(err, aiservice.ErrForbidden):
		c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": aiservice.ErrDatabase.Error()})
	}
}
