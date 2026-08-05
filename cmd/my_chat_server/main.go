package main

import (
	"mychat/internal/config"
	"mychat/internal/dao"
	"mychat/internal/https_server"
	"mychat/internal/service/gormservice"
)

func main() {
	cfg, err := config.LoadConfig("config/config.toml")
	if err != nil {
		panic(err)
	}
	gormservice.InitJWT(cfg.JWTConfig)
	dao.InitGorm(cfg.MySQLConfig)
	defer dao.CloseGorm()

	r := https_server.NewEngine(cfg.JWTConfig.Secret)
	r.Run(":8080")
}
