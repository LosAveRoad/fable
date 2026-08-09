package auth

import (
	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	UserUUID string `json:"user_uuid"`
	jwt.RegisteredClaims
}

func ParseJWT(rawToken string, jwtKey []byte) (*Claims, error) {
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
		jwt.WithExpirationRequired(),
	)
	if err != nil || token == nil || !token.Valid || claims.UserUUID == "" {
		return nil, jwt.ErrTokenInvalidClaims
	}
	return claims, nil
}
