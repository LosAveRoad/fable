package main

import (
	"log"
	"net/http"

	"mychat/internal/config"
	"mychat/internal/dao"
	"mychat/internal/https_server"
	"mychat/internal/mcpserver"
	"mychat/internal/service/gormservice"
)

func main() {
	cfg, err := config.LoadConfig("config/config.toml")
	if err != nil {
		panic(err)
	}
	gormservice.InitJWT(cfg.JWTConfig)
	if err := dao.InitGorm(cfg.MySQLConfig); err != nil {
		panic(err)
	}
	defer dao.CloseGorm()

	ginHandler := https_server.NewEngine(cfg.JWTConfig.Secret)
	mcpHandler := mcpserver.NewHTTPHandler(mcpserver.New(), cfg.JWTConfig.Secret)

	root := http.NewServeMux()
	root.Handle("/mcp", mcpHandler)
	root.Handle("/", ginHandler)

	log.Println("chat and MCP server listening on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", root))
}
