package v1

import (
	"errors"
	"mychat/internal/dto/request"
	gormservice "mychat/internal/service/gormservice"
	"net/http"

	"github.com/gin-gonic/gin"
)

func Register(c *gin.Context) {
	var req request.RegisterRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid Register Request",
		})
		return
	}

	user, err := gormservice.Register(&req)

	if err != nil {
		switch {
		case errors.Is(err, gormservice.ErrInvalidRegister):
			c.JSON(http.StatusConflict, err)
		case errors.Is(err, gormservice.ErrUsernameExists):
			c.JSON(http.StatusConflict, err)
		case errors.Is(err, gormservice.ErrTelephoneExists):
			c.JSON(http.StatusConflict, err)
		case errors.Is(err, gormservice.ErrCreateUserFailed):
			c.JSON(http.StatusBadRequest, err)
		default:
			c.JSON(http.StatusBadGateway, err)
		}
		return
	}

	c.JSON(http.StatusAccepted, user)
}

func Login(c *gin.Context) {
	var req request.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, err)
		return
	}

	user, err := gormservice.Login(&req)

	if err != nil {
		switch {
		case errors.Is(err, gormservice.ErrUserNotFound):
			c.JSON(http.StatusBadRequest, err)
		case errors.Is(err, gormservice.ErrInvalidPassword):
			c.JSON(http.StatusBadRequest, err)
		case errors.Is(err, gormservice.ErrLoginFailed):
			c.JSON(http.StatusInternalServerError, err)
		default:
			c.JSON(http.StatusInternalServerError, err)
		}
	}

	c.JSON(http.StatusOK, user)
}

func GetUserInfo(c *gin.Context) {
	var req request.GetUserInfoRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, err)
		return
	}

	uuid, ok := c.Get("user_uuid")
	if !ok {
		c.JSON(http.StatusForbidden, "you haven't loggin")
		return
	}

	user, err := gormservice.GetUserInfo(&req, uuid)

	if err != nil {
		switch {
		case errors.Is(gormservice.ErrInvalidUUID, err):
			c.JSON(http.StatusConflict, err)
		case errors.Is(gormservice.ErrForbidden, err):
			c.JSON(http.StatusForbidden, err)
		case errors.Is(gormservice.ErrUserAccessDenied, err):
			c.JSON(http.StatusForbidden, err)
		default:
			c.JSON(http.StatusInternalServerError, err)
		}
		return
	}

	c.JSON(http.StatusOK, user)

}
