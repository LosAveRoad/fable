package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mychat/internal/config"
	"mychat/internal/dao"
	"mychat/internal/dto/wschat"
	"mychat/internal/https_server"
	"mychat/internal/mcpserver"
	"mychat/internal/service/chatservice"
	"mychat/internal/service/gormservice"
	"mychat/internal/service/kafkaservice"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	errCh := make(chan error, 2)
	cfg, err := config.LoadConfig("config/config.toml")
	if err != nil {
		return err
	}
	gormservice.InitJWT(cfg.JWTConfig)
	if err := dao.InitGorm(cfg.MySQLConfig); err != nil {
		return err
	}
	defer dao.CloseGorm()
	if cfg.RedisConfig.Enabled {
		if err := dao.InitRedis(cfg.RedisConfig); err != nil {
			if cfg.RedisConfig.Required {
				return err
			}
			log.Printf("redis disabled after initialization failure: %v", err)
		}
		defer dao.CloseRedis()
	}
	var kafkaSvc *kafkaservice.Service
	if cfg.KafkaConfig.Enabled && cfg.KafkaConfig.Mode == "kafka" {
		kafkaSvc, err = kafkaservice.New(cfg.KafkaConfig)
		if err != nil {
			if cfg.KafkaConfig.Required {
				return err
			}
			log.Printf("kafka disabled after initialization failure: %v", err)
		} else {
			chatservice.PublishChatEvent = kafkaSvc.Publish
			go func() {
				if consumeErr := kafkaSvc.Consume(ctx, func(_ context.Context, event wschat.ChatEvent) error {
					return chatservice.ChatServer.DeliverEvent(event)
				}); consumeErr != nil && ctx.Err() == nil {
					errCh <- consumeErr
				}
			}()
			defer func() { chatservice.PublishChatEvent = nil; _ = kafkaSvc.Close() }()
		}
	}
	defer chatservice.ChatServer.Close()
	go chatservice.ChatServer.Start()

	ginHandler := https_server.NewEngine(cfg.JWTConfig.Secret)
	mcpHandler := mcpserver.NewHTTPHandler(mcpserver.New(), cfg.JWTConfig.Secret)

	root := http.NewServeMux()
	root.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	root.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if dao.GormDB == nil {
			http.Error(w, "mysql unavailable", http.StatusServiceUnavailable)
			return
		}
		sqlDB, err := dao.GormDB.DB()
		if err != nil || sqlDB.PingContext(ctx) != nil {
			http.Error(w, "mysql unavailable", http.StatusServiceUnavailable)
			return
		}
		if cfg.RedisConfig.Required && (dao.RedisClient == nil || dao.RedisClient.Ping(ctx).Err() != nil) {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		if cfg.KafkaConfig.Required && (kafkaSvc == nil || kafkaSvc.Ready(ctx) != nil) {
			http.Error(w, "kafka unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	root.Handle("/mcp", mcpHandler)
	root.Handle("/", ginHandler)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           root,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
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

	shutdownTimeout := 25 * time.Second
	if os.Getenv("FABLE_FAST_SHUTDOWN") == "true" {
		shutdownTimeout = 5 * time.Second
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
