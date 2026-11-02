package blackbox

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestUnsafeTargetsRejectedBeforeTraffic(t *testing.T) {
	for _, base := range []string{"", "http://example.com", "http://127.0.0.1:80/?token=secret", "https://u:p@example.com", "https://example.com/#token", "ftp://localhost", "https://example.com/path", "https://example.com?"} {
		if _, err := newClient(Config{BaseURL: base}); err == nil {
			t.Fatalf("accepted unsafe target %q", base)
		}
	}
}
func TestTLSValidatesCAAndHostname(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) }))
	defer server.Close()
	c, err := newClient(Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	if _, err = c.request(context.Background(), "GET", "/", "", nil, nil); err == nil {
		t.Fatal("untrusted TLS accepted")
	}
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	path := t.TempDir() + "/ca.pem"
	if err = os.WriteFile(path, cert, 0600); err != nil {
		t.Fatal(err)
	}
	c, err = newClient(Config{BaseURL: server.URL, CAFile: path})
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	if _, err = c.request(context.Background(), "GET", "/", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	wrong := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	bad, err := newClient(Config{BaseURL: wrong, CAFile: path})
	if err != nil {
		t.Fatal(err)
	}
	defer bad.close()
	if _, err = bad.request(context.Background(), "GET", "/", "", nil, nil); err == nil {
		t.Fatal("hostname mismatch accepted")
	}
}
func TestNoRedirectOrRawErrorLeak(t *testing.T) {
	hits := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(500)
		w.Write([]byte("private-token secret-password"))
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }))
	defer source.Close()
	c, _ := newClient(Config{BaseURL: source.URL})
	defer c.close()
	status, err := c.request(context.Background(), "GET", "/", "secret-token", nil, nil)
	if err != nil || status != 302 || hits != 0 {
		t.Fatalf("redirect followed: status=%d hits=%d err=%v", status, hits, err)
	}
	c2, _ := newClient(Config{BaseURL: target.URL})
	defer c2.close()
	_, err = c2.request(context.Background(), "GET", "/", "", nil, &struct{}{})
	if err != nil && strings.Contains(err.Error(), "private-token") {
		t.Fatal("raw body leaked")
	}
}
func TestRequestCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	c, _ := newClient(Config{BaseURL: server.URL})
	defer c.close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.request(ctx, "GET", "/", "", nil, nil); err == nil {
		t.Fatal("canceled request succeeded")
	}
}
func TestIdentityUsesBoundedDistinctRandomCredentials(t *testing.T) {
	a, err := makeIdentity()
	if err != nil {
		t.Fatal(err)
	}
	b, err := makeIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^1[3-9]\d{9}$`).MatchString(a.telephone) {
		t.Fatal("phone rejected by public registration contract")
	}
	if a.telephone == b.telephone || a.password == b.password || len(a.telephone) > 24 || len(a.nickname) > 20 || len(a.password) < 20 {
		t.Fatal("invalid fixture identity")
	}
	raw, _ := json.Marshal(a)
	if strings.Contains(string(raw), a.password) || strings.Contains(string(raw), a.telephone) {
		t.Fatal("credentials serialize")
	}
}
func TestLoadLimitsAndNearestRankPercentiles(t *testing.T) {
	for _, cfg := range []LoadConfig{{Pairs: 33}, {Rate: 201}, {Duration: 6 * time.Minute}, {Pairs: -1}, {Rate: -1}, {MinReceiveRatio: 1.1}} {
		if _, err := cfg.normalized(); err == nil {
			t.Fatalf("accepted limits: %+v", cfg)
		}
	}
	p := summarize([]time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond, 100 * time.Millisecond})
	if p.P50MS != 3 || p.P95MS != 100 || p.P99MS != 100 {
		t.Fatalf("incorrect latency: %+v", p)
	}
}

func TestSyncRejectsNoncanonicalPosition(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"conversation_id":"direct:S1","messages":[{"message_id":"M1","conversation_id":"direct:S1","position":"1oops"}]}`))
	}))
	defer server.Close()
	c, _ := newClient(Config{BaseURL: server.URL})
	defer c.close()
	if _, err := c.sync(context.Background(), identity{}, "direct:S1", 2); err == nil {
		t.Fatal("accepted malformed authoritative position")
	}
}
