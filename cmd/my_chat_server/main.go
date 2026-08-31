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
					return chatservice.ChatServer.HandleMessage(event.SenderID, wschat.Message{SendID: event.SenderID, ReceiveID: event.ReceiveID, ReceiveType: event.ReceiveType, Content: event.Content})
				}); consumeErr != nil && ctx.Err() == nil {
					log.Printf("kafka consumer stopped: %v", consumeErr)
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
	root.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
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
