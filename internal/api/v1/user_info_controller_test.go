package v1

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRegisterRejectsInvalidJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/register",
		strings.NewReader(`{"telephone":`),
	)
	request.Header.Set("Content-Type", "application/json")

	context, _ := gin.CreateTestContext(recorder)
	context.Request = request

	Register(context)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestLoginRejectsInvalidJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/login",
		strings.NewReader(`{"telephone":`),
	)
	request.Header.Set("Content-Type", "application/json")

	context, _ := gin.CreateTestContext(recorder)
	context.Request = request

	Login(context)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}
