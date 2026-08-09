package https_server

import (
	"net/http"
	"strings"

	appauth "mychat/internal/auth"

	"github.com/gin-gonic/gin"
)

func abortUnauthorized(c *gin.Context, message string) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
		"error": message,
	})
}

func Auth(jwtKey []byte) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			abortUnauthorized(c, "missing authorization header")
			return
		}

		parts := strings.Fields(authHeader)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
			abortUnauthorized(c, "invalid authorization header")
			return
		}

		claims, err := appauth.ParseJWT(parts[1], jwtKey)
		if err != nil {
			abortUnauthorized(c, "invalid or expired token")
			return
		}

		c.Set("user_uuid", claims.UserUUID)
		c.Next()
	}
}

func WsAuth(jwtKey []byte) gin.HandlerFunc {
	return func(c *gin.Context) {
		rawToken := strings.TrimSpace(c.Query("token"))
		if rawToken == "" {
			parts := strings.Fields(c.GetHeader("Authorization"))
			if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
				rawToken = parts[1]
			}
		}
		if rawToken == "" {
			abortUnauthorized(c, "missing websocket token")
			return
		}

		claims, err := appauth.ParseJWT(rawToken, jwtKey)
		if err != nil {
			abortUnauthorized(c, "invalid or expired token")
			return
		}

		if clientID := strings.TrimSpace(c.Query("client_id")); clientID != "" && clientID != claims.UserUUID {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "client_id does not match token user",
			})
			return
		}

		c.Set("user_uuid", claims.UserUUID)
		// WsController currently reads user_id; keep both keys during the MVP0 transition.
		c.Set("user_id", claims.UserUUID)
		c.Next()
	}
}
