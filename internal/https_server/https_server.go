package https_server

import (
	v1 "mychat/internal/api/v1"

	"github.com/gin-gonic/gin"
)

func NewEngine(jwtKey []byte) *gin.Engine {
	r := gin.Default()
	r.Static("/chat", "./web/chat-server")

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
	r.POST(
		"/message/getMessageList",
		Auth(jwtKey),
		v1.GetMessageList,
	)
	r.POST("/group/create", Auth(jwtKey), v1.CreateGroup)
	r.POST("/group/join", Auth(jwtKey), v1.JoinGroup)
	r.POST("/group/join/approve", Auth(jwtKey), v1.ApproveGroupJoin)
	r.POST("/group/leave", Auth(jwtKey), v1.LeaveGroup)
	r.POST("/group/info", Auth(jwtKey), v1.GetGroupInfo)
	r.POST("/group/list/joined", Auth(jwtKey), v1.GetJoinedGroupList)
	r.POST("/group/list/owned", Auth(jwtKey), v1.GetOwnedGroupList)
	r.POST("/group/message/list", Auth(jwtKey), v1.GetGroupMessageList)
	r.POST("/session/getGroupSessionList", Auth(jwtKey), v1.GetGroupSessionList)
	r.POST("/contact/list/users", Auth(jwtKey), v1.GetContactUserList)
	r.GET(
		"/api/v1/ai/setting",
		Auth(jwtKey),
		v1.GetAISetting,
	)
	r.PUT(
		"/api/v1/ai/setting",
		Auth(jwtKey),
		v1.ChangeAISetting,
	)
	r.GET("/wss", WsAuth(jwtKey), v1.WsController)
	return r
}
