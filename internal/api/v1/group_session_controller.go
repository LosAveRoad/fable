package v1

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"mychat/internal/service/gormservice"
)

func GetGroupSessionList(c *gin.Context) {
	id, ok := currentUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	result, err := gormservice.GetGroupSessionList(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}
