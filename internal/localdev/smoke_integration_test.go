//go:build integration

package localdev

import (
	"context"
	"database/sql"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	logicapp "mychat/internal/logic/app"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestSelectedTopologySmokeBidirectionalThroughIngress(t *testing.T) {
	file, entry := os.Getenv("FABLE_LOCAL_SMOKE_CONFIG"), os.Getenv("FABLE_LOCAL_SMOKE_ENTRY")
	if file == "" || entry == "" {
		t.Skip("selected running local topology required")
	}
	c, err := logicapp.LoadConfig(file)
	if err != nil {
		t.Fatal(err)
	}
	assertUnchangedSmokeCounts(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = Smoke(ctx, c, entry); err != nil {
		t.Fatal(err)
	}
}

func TestSelectedSmokeCancellationClosesStalledSocket(t *testing.T) {
	file := os.Getenv("FABLE_LOCAL_SMOKE_CONFIG")
	if file == "" {
		t.Skip("selected local database required")
	}
	c, err := logicapp.LoadConfig(file)
	if err != nil {
		t.Fatal(err)
	}
	assertUnchangedSmokeCounts(t, c)
	connected := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer socket.Close()
		connected <- struct{}{}
		for {
			if _, _, err = socket.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Smoke(ctx, c, server.URL) }()
	for i := 0; i < 2; i++ {
		select {
		case <-connected:
		case <-time.After(3 * time.Second):
			t.Fatal("smoke did not open selected ingress socket")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled smoke passed")
		}
	case <-time.After(time.Second):
		<-done // Let deferred fixture cleanup finish before reporting RED.
		t.Fatal("smoke ignored cancellation while awaiting frames")
	}
}

func assertUnchangedSmokeCounts(t *testing.T, c logicapp.Config) {
	t.Helper()
	cfg := mysqlDriver.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(c.Base.MySQLConfig.Host, strconv.Itoa(c.Base.MySQLConfig.Port))
	cfg.User = c.Base.MySQLConfig.User
	cfg.Passwd = c.Base.MySQLConfig.Password
	cfg.DBName = c.Base.MySQLConfig.DatabaseName
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	counts := func() [3]int {
		var v [3]int
		if err := db.QueryRow("SELECT (SELECT COUNT(*) FROM user_info),(SELECT COUNT(*) FROM session),(SELECT COUNT(*) FROM message)").Scan(&v[0], &v[1], &v[2]); err != nil {
			t.Fatal(err)
		}
		return v
	}
	before := counts()
	t.Cleanup(func() {
		defer db.Close()
		if after := counts(); after != before {
			t.Fatalf("smoke left fixture rows: before=%v after=%v", before, after)
		}
	})
}
