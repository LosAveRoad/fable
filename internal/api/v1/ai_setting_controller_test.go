package v1

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestGetAISettingRejectsMissingIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodGet, "/api/v1/ai/setting", nil)

	GetAISetting(context)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
}

func TestChangeAISettingRejectsMissingAllowedSessions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, body := range []string{`{}`, `{"allowed_session_uuids":null}`} {
		t.Run(body, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(recorder)
			context.Set("user_uuid", "U001")
			context.Request = httptest.NewRequest(http.MethodPut, "/api/v1/ai/setting", strings.NewReader(body))
			context.Request.Header.Set("Content-Type", "application/json")

			ChangeAISetting(context)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
			}
		})
	}
}
