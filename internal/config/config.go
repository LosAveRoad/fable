package config

import "github.com/BurntSushi/toml"

type MySQLConfig struct {
	Host         string `toml:"host"`
	Port         int    `toml:"port"`
	User         string `toml:"user"`
	Password     string `toml:"password"`
	DatabaseName string `toml:"databaseName"`
}

type JWTConfig struct {
	Secret []byte `toml:"secret"`
}

type Config struct {
	MySQLConfig MySQLConfig `toml:"mysqlConfig"`
	JWTConfig   JWTConfig   `toml:"jwtConfig"`
}

func LoadConfig(path string) (*Config, error) {
	var cfg Config

	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
