package https_server

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	UserUUID string `json:"user_uuid"`
	jwt.RegisteredClaims
}

func abortUnauthorized(c *gin.Context, message string) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
		"error": message,
	})
}

func parseJWTClaims(rawToken string, jwtKey []byte) (*Claims, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(
		rawToken,
		claims,
		func(token *jwt.Token) (any, error) {
			if token.Method != jwt.SigningMethodHS256 {
				return nil, jwt.ErrSignatureInvalid
			}
			return jwtKey, nil
		},
	)
	if err != nil || token == nil || !token.Valid || claims.UserUUID == "" {
		return nil, jwt.ErrTokenInvalidClaims
	}
	return claims, nil
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

		claims, err := parseJWTClaims(parts[1], jwtKey)
		if err != nil {
			abortUnauthorized(c, "invalid or expired token")
			return
		}

		c.Set("user_uuid", claims.UserUUID)
		c.Next()
	}
}

// WsAuth authenticates a browser WebSocket handshake before the connection is upgraded.
// The token is read from ?token= because browsers cannot set Authorization headers
// on a native WebSocket constructor. An Authorization header is also accepted for
// non-browser clients. If client_id is supplied, it must match the JWT identity.
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

		claims, err := parseJWTClaims(rawToken, jwtKey)
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
