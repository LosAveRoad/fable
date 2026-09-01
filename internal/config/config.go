package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

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

type RedisConfig struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Password string `toml:"password"`
	DB       int    `toml:"db"`
	Enabled  bool   `toml:"enabled"`
	Required bool   `toml:"required"`
}

type KafkaConfig struct {
	Enabled        bool     `toml:"enabled"`
	Required       bool     `toml:"required"`
	Mode           string   `toml:"mode"`
	Brokers        []string `toml:"brokers"`
	Topic          string   `toml:"topic"`
	ConsumerGroup  string   `toml:"consumerGroup"`
	PartitionCount int      `toml:"partitionCount"`
	InstanceID     string   `toml:"-"`
}

type Config struct {
	MySQLConfig MySQLConfig `toml:"mysqlConfig"`
	JWTConfig   JWTConfig   `toml:"jwtConfig"`
	RedisConfig RedisConfig `toml:"redisConfig"`
	KafkaConfig KafkaConfig `toml:"kafkaConfig"`
}

func LoadConfig(path string) (*Config, error) {
	var cfg Config

	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, err
	}
	applyEnvOverrides(&cfg)

	return &cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("FABLE_MYSQL_HOST"); v != "" {
		cfg.MySQLConfig.Host = v
	}
	if v := os.Getenv("FABLE_MYSQL_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MySQLConfig.Port = n
		}
	}
	if v := os.Getenv("FABLE_MYSQL_USER"); v != "" {
		cfg.MySQLConfig.User = v
	}
	if v := os.Getenv("FABLE_MYSQL_PASSWORD"); v != "" {
		cfg.MySQLConfig.Password = v
	}
	if v := os.Getenv("FABLE_MYSQL_DATABASE"); v != "" {
		cfg.MySQLConfig.DatabaseName = v
	}
	if v := os.Getenv("FABLE_JWT_SECRET"); v != "" {
		cfg.JWTConfig.Secret = []byte(v)
	}
	if v := os.Getenv("FABLE_REDIS_HOST"); v != "" {
		cfg.RedisConfig.Host = v
	}
	if v := os.Getenv("FABLE_REDIS_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.RedisConfig.Port = n
		}
	}
	if v := os.Getenv("FABLE_REDIS_PASSWORD"); v != "" {
		cfg.RedisConfig.Password = v
	}
	if v := os.Getenv("FABLE_REDIS_DB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.RedisConfig.DB = n
		}
	}
	if v := os.Getenv("FABLE_REDIS_ENABLED"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.RedisConfig.Enabled = b
		}
	}
	if v := os.Getenv("FABLE_REDIS_REQUIRED"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.RedisConfig.Required = b
		}
	}
	if v := os.Getenv("FABLE_KAFKA_ENABLED"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.KafkaConfig.Enabled = b
		}
	}
	if v := os.Getenv("FABLE_KAFKA_REQUIRED"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.KafkaConfig.Required = b
		}
	}
	if v := os.Getenv("FABLE_KAFKA_MODE"); v != "" {
		cfg.KafkaConfig.Mode = v
	}
	if v := os.Getenv("FABLE_KAFKA_BROKERS"); v != "" {
		cfg.KafkaConfig.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv("FABLE_KAFKA_TOPIC"); v != "" {
		cfg.KafkaConfig.Topic = v
	}
	if v := os.Getenv("FABLE_KAFKA_CONSUMER_GROUP"); v != "" {
		cfg.KafkaConfig.ConsumerGroup = v
	}
	if v := os.Getenv("FABLE_INSTANCE_ID"); v != "" {
		cfg.KafkaConfig.InstanceID = v
	}
}
