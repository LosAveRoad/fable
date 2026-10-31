package app

import (
	"context"
	"google.golang.org/grpc"
	"gorm.io/gorm"
	"mychat/internal/dao"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestRunCanceledBeforeStartupDoesNotInstallGlobals(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, validConfig()); err != nil {
		t.Fatal(err)
	}
	if dao.GormDB != nil {
		t.Fatal("canceled startup installed DB")
	}
}
func TestRunRefusesAlreadyInstalledDatabase(t *testing.T) {
	old := dao.GormDB
	dao.GormDB = &gorm.DB{}
	defer func() { dao.GormDB = old }()
	if err := Run(context.Background(), validConfig()); err == nil {
		t.Fatal("existing process DB overwritten")
	}
}
func TestServersDrainInflightHTTPBeforeShutdown(t *testing.T) {
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; w.WriteHeader(204) })}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, server, grpc.NewServer(), httpListener, rpcListener) }()
	requestDone := make(chan error, 1)
	go func() {
		r, e := http.Get("http://" + httpListener.Addr().String())
		if e == nil {
			r.Body.Close()
		}
		requestDone <- e
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("server never started")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("stopped before in-flight response: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish")
	}
	if conn, e := net.DialTimeout("tcp", rpcListener.Addr().String(), time.Millisecond*50); e == nil {
		conn.Close()
		t.Fatal("RPC listener remained open")
	}
}
