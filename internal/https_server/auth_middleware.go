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

		claims := &Claims{}
		token, err := jwt.ParseWithClaims(
			parts[1],
			claims,
			func(token *jwt.Token) (any, error) {
				if token.Method != jwt.SigningMethodHS256 {
					return nil, jwt.ErrSignatureInvalid
				}
				return jwtKey, nil
			},
		)
		if err != nil || token == nil || !token.Valid {
			abortUnauthorized(c, "invalid or expired token")
			return
		}

		if claims.UserUUID == "" {
			abortUnauthorized(c, "missing user uuid")
			return
		}

		c.Set("user_uuid", claims.UserUUID)
		c.Next()
	}
}
