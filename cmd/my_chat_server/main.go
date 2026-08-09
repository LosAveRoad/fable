package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"mychat/internal/config"
	"mychat/internal/dao"
	"mychat/internal/https_server"
	"mychat/internal/mcpserver"
	"mychat/internal/service/chatservice"
	"mychat/internal/service/gormservice"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.LoadConfig("config/config.toml")
	if err != nil {
		return err
	}
	gormservice.InitJWT(cfg.JWTConfig)
	if err := dao.InitGorm(cfg.MySQLConfig); err != nil {
		return err
	}
	defer dao.CloseGorm()
	defer chatservice.ChatServer.Close()
	go chatservice.ChatServer.Start()

	ginHandler := https_server.NewEngine(cfg.JWTConfig.Secret)
	mcpHandler := mcpserver.NewHTTPHandler(mcpserver.New(), cfg.JWTConfig.Secret)

	root := http.NewServeMux()
	root.Handle("/mcp", mcpHandler)
	root.Handle("/", ginHandler)

	server := &http.Server{
		Addr:    ":8080",
		Handler: root,
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	log.Println("chat and MCP server listening on http://localhost:8080")
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
