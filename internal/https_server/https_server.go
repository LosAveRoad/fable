package https_server

import (
	v1 "mychat/internal/api/v1"

	"github.com/gin-gonic/gin"
)

func NewEngine(jwtKey []byte) *gin.Engine {
	r := gin.Default()

	r.POST("/register", v1.Register)
	r.POST("/login", v1.Login)
	r.POST(
		"/user/getUserInfo",
		Auth(jwtKey),
		v1.GetUserInfo,
	)
	r.POST(
		"/session/openSession",
		Auth(jwtKey),
		v1.OpenSession,
	)
	r.POST(
		"/session/getUserSessionList",
		Auth(jwtKey),
		v1.GetUserSessionList,
	)
	return r
}
