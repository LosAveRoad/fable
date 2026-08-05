package https_server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

const testJWTSecret = "test-secret"

func makeTestToken(t *testing.T, secret string, userUUID string, expiresAt time.Time) string {
	t.Helper()

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
		UserUUID: userUUID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	})

	tokenString, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("create test token: %v", err)
	}
	return tokenString
}

func TestAuthRejectsInvalidRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name          string
		authorization string
		secret        string
	}{
		{
			name:          "missing header",
			authorization: "",
			secret:        testJWTSecret,
		},
		{
			name:          "invalid header format",
			authorization: "Basic abc",
			secret:        testJWTSecret,
		},
		{
			name:          "malformed token",
			authorization: "Bearer not-a-jwt",
			secret:        testJWTSecret,
		},
		{
			name:          "wrong secret",
			authorization: "Bearer " + makeTestToken(t, "another-secret", "U001", time.Now().Add(time.Hour)),
			secret:        testJWTSecret,
		},
		{
			name:          "expired token",
			authorization: "Bearer " + makeTestToken(t, testJWTSecret, "U001", time.Now().Add(-time.Hour)),
			secret:        testJWTSecret,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			nextCalled := false
			router.GET("/protected", Auth([]byte(tt.secret)), func(c *gin.Context) {
				nextCalled = true
				c.Status(http.StatusOK)
			})

			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/protected", nil)
			if tt.authorization != "" {
				request.Header.Set("Authorization", tt.authorization)
			}
			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
			}
			if nextCalled {
				t.Fatal("next handler was called for an invalid request")
			}
		})
	}
}

func TestAuthPassesUserUUIDToNextHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	var userUUIDFromContext any
	router.GET("/protected", Auth([]byte(testJWTSecret)), func(c *gin.Context) {
		userUUIDFromContext, _ = c.Get("user_uuid")
		c.Status(http.StatusOK)
	})

	recorder := httptest.NewRecorder()
	token := makeTestToken(t, testJWTSecret, "U001", time.Now().Add(time.Hour))
	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}

	// Auth 调用 c.Next() 时，测试上下文中没有预先注册后续 Handler，
	// 这里由后续 Handler 读取中间件写入的 Context 值。
	if userUUIDFromContext == nil {
		t.Fatal("user_uuid was not written to context")
	}
	userUUID, ok := userUUIDFromContext.(string)
	if !ok || userUUID != "U001" {
		t.Fatalf("user_uuid = %v, want U001", userUUIDFromContext)
	}
}
