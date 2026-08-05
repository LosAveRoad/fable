package v1

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestGetMessageListRejectsNonParticipant(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(
		http.MethodPost,
		"/message/getMessageList",
		strings.NewReader(`{"user_one_id":"U001","user_two_id":"U002"}`),
	)
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("user_uuid", "U003")

	GetMessageList(c)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
}

func TestGetMessageListRejectsMissingIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(
		http.MethodPost,
		"/message/getMessageList",
		strings.NewReader(`{"user_one_id":"U001","user_two_id":"U002"}`),
	)
	c.Request.Header.Set("Content-Type", "application/json")

	GetMessageList(c)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
}
