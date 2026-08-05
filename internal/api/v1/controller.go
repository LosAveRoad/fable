// Package v1 包含第一版 API 的 HTTP 和 WebSocket Handler。
//
// 本包中的 Handler 只负责接入层工作：解析并校验请求、从 gin.Context
// 读取已认证用户、调用业务 Service，以及编码响应。业务规则和 GORM
// 查询不应写在本包中。
package v1

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Response 是 MVP0 HTTP API 的统一成功响应结构。
// 当接口没有响应数据时，Data 字段不会出现在 JSON 中。
type Response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// ErrorResponse 是请求失败时返回的统一 JSON 结构。
type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// notImplemented 表示该 API 契约的业务逻辑尚未实现。
// 实现对应业务切片时，应逐个替换对该函数的调用。
func notImplemented(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, ErrorResponse{
		Code:    http.StatusNotImplemented,
		Message: "not implemented",
	})
}
