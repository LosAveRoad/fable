package https_server

import (
	"github.com/gin-gonic/gin"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStandaloneLogicDoesNotServeLegacyChatAssets(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "web", "chat-server")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "index.html"), []byte("legacy-only-fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	legacy := httptest.NewRecorder()
	NewEngine([]byte("test-key")).ServeHTTP(legacy, httptest.NewRequest("GET", "/chat/", nil))
	if legacy.Code != 200 || !strings.Contains(legacy.Body.String(), "legacy-only-fixture") {
		t.Fatalf("legacy regression entry lost assets: %d", legacy.Code)
	}
	modern := httptest.NewRecorder()
	NewLogicEngine([]byte("test-key"), func(c *gin.Context) { c.Status(200) }).ServeHTTP(modern, httptest.NewRequest("GET", "/chat/", nil))
	if modern.Code != 404 {
		t.Fatalf("new Logic exposed old client: %d", modern.Code)
	}
}
