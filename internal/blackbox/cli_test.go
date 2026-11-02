package blackbox

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestCLIRequiresExplicitReportBeforeTraffic(t *testing.T) {
	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits++ }))
	defer server.Close()
	var stderr bytes.Buffer
	if code := CLI(context.Background(), []string{"-base-url", server.URL}, false, &stderr); code != 2 || hits != 0 {
		t.Fatalf("code=%d hits=%d", code, hits)
	}
}
func TestCLIFailureReportIsPrivateAndSanitized(t *testing.T) {
	path := t.TempDir() + "/report.json"
	var stderr bytes.Buffer
	if code := CLI(context.Background(), []string{"-base-url", "https://user:secret-token@example.com", "-report", path}, false, &stderr); code != 1 {
		t.Fatalf("exit code %d", code)
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var report Report
	if json.Unmarshal(raw, &report) != nil || report.Passed || report.Error == "" {
		t.Fatalf("invalid failure report: %s", raw)
	}
	if strings.Contains(string(raw)+stderr.String(), "secret-token") {
		t.Fatal("credentials leaked into report or diagnostics")
	}
	info, e := os.Stat(path)
	if e != nil {
		t.Fatal(e)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatal("report is not owner-readable only")
	}
}
