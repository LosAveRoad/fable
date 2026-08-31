package v1

import (
	"github.com/gin-gonic/gin"
	"mychat/internal/service/gormservice"
	"net/http"
)

func GetContactUserList(c *gin.Context) {
	id, ok := currentUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	r, err := gormservice.GetContactUserList(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, r)
}
