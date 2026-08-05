package v1

import "github.com/gin-gonic/gin"

// WsLogin 处理 GET /wss，并将 HTTP 连接升级为 WebSocket。
//
// 鉴权：必须在 Upgrade 之前完成。
//
// 查询参数：
//
//	/wss?client_id=U001&token=<access-token>
//
// MVP0 保留 KamaChat 的 client_id 命名，但 Handler 必须验证它与 Token
// 中的 uuid 完全一致。鉴权成功后，Handler 升级连接、创建一个 chat.Client、
// 将其注册到 chat.Server，并且只启动一个读循环和一个写循环。
//
// 成功响应：101 Switching Protocols。
//
// Upgrade 前的错误响应：400 缺少参数；401 Token 无效或已过期；
// 403 client_id 与 Token 身份不一致；500 升级或服务器内部失败。
func WsLogin(c *gin.Context) {
	notImplemented(c)
}
