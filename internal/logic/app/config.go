// Package app wires the standalone Logic process without legacy WS ownership.
package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
	"mychat/internal/config"
	"mychat/internal/logic"
	"mychat/internal/logic/kafkapush"
)

type Config struct {
	Base                                                      config.Config
	HTTPAddress, RPCAddress, RedisPrefix, JobToken, KickToken string
	Comets                                                    []logic.CometGrant
}

var errConfig = errors.New("invalid Logic configuration; check required settings and authorization grants")

func LoadConfig(path string) (Config, error) {
	base, err := config.LoadConfig(path)
	if err != nil {
		return Config{}, errors.New("cannot load Logic configuration")
	}
	var file struct {
		Logic struct {
			HTTPAddress string `toml:"httpAddress"`
			RPCAddress  string `toml:"rpcAddress"`
			RedisPrefix string `toml:"redisPrefix"`
		} `toml:"logic"`
	}
	if _, err := toml.DecodeFile(path, &file); err != nil {
		return Config{}, errors.New("cannot load Logic configuration")
	}
	c := Config{Base: *base, HTTPAddress: file.Logic.HTTPAddress, RPCAddress: file.Logic.RPCAddress, RedisPrefix: file.Logic.RedisPrefix}
	overlay := func(name string, dst *string) {
		if value, ok := os.LookupEnv(name); ok {
			*dst = value
		}
	}
	overlay("FABLE_LOGIC_HTTP_ADDR", &c.HTTPAddress)
	overlay("FABLE_LOGIC_RPC_ADDR", &c.RPCAddress)
	overlay("FABLE_LOGIC_REDIS_PREFIX", &c.RedisPrefix)
	overlay("FABLE_LOGIC_JOB_TOKEN", &c.JobToken)
	overlay("FABLE_LOGIC_KICK_TOKEN", &c.KickToken)
	if value := os.Getenv("FABLE_LOGIC_COMET_GRANTS_JSON"); value != "" {
		dec := json.NewDecoder(bytes.NewBufferString(value))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&c.Comets); err != nil {
			return Config{}, errConfig
		}
		if err := dec.Decode(new(any)); err != io.EOF {
			return Config{}, errConfig
		}
	}
	// The legacy loader ignores malformed numeric environment overrides. Logic
	// rejects them so an invalid explicitly selected backend cannot be hidden.
	for _, name := range []string{"FABLE_MYSQL_PORT", "FABLE_REDIS_PORT", "FABLE_REDIS_DB"} {
		if value, ok := os.LookupEnv(name); ok {
			if _, err := strconv.Atoi(value); err != nil {
				return Config{}, errConfig
			}
		}
	}
	for _, name := range []string{"FABLE_REDIS_ENABLED", "FABLE_REDIS_REQUIRED", "FABLE_KAFKA_ENABLED", "FABLE_KAFKA_REQUIRED"} {
		if value, ok := os.LookupEnv(name); ok {
			if _, err := strconv.ParseBool(value); err != nil {
				return Config{}, errConfig
			}
		}
	}
	if err := validate(c); err != nil {
		return Config{}, err
	}
	return c, nil
}
func token(value string) bool {
	if len(value) < 32 || len(value) > 256 {
		return false
	}
	for _, r := range value {
		if r <= 32 || r > 126 {
			return false
		}
	}
	return true
}
func loopback(address string, allowZero bool) bool {
	host, port, err := net.SplitHostPort(address)
	n, e := strconv.Atoi(port)
	ip := net.ParseIP(host)
	return err == nil && e == nil && n >= 0 && n <= 65535 && (allowZero || n > 0) && (host == "localhost" || ip != nil && ip.IsLoopback())
}
func databaseValid(c config.MySQLConfig) bool {
	if c.Host == "" || c.Port < 1 || c.Port > 65535 || c.User == "" || c.DatabaseName == "" {
		return false
	}
	for _, r := range c.DatabaseName {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_') {
			return false
		}
	}
	switch strings.ToLower(c.DatabaseName) {
	case "mysql", "sys", "information_schema", "performance_schema":
		return false
	}
	return true
}
func validate(c Config) error {
	if !c.Base.RedisConfig.Enabled || !c.Base.KafkaConfig.Enabled || c.Base.KafkaConfig.Mode != "kafka" || !databaseValid(c.Base.MySQLConfig) || !loopback(c.HTTPAddress, true) || !loopback(c.RPCAddress, true) || c.HTTPAddress == c.RPCAddress || len(c.Base.JWTConfig.Secret) < 32 || !token(c.JobToken) || !token(c.KickToken) || c.JobToken == c.KickToken || len(c.Comets) == 0 || c.Base.RedisConfig.Host == "" || c.Base.RedisConfig.Port < 1 || c.Base.RedisConfig.Port > 65535 || c.Base.RedisConfig.DB < 0 || len(c.RedisPrefix) > 256 || strings.ContainsAny(c.RedisPrefix, "{}\r\n\x00") {
		return errConfig
	}
	tokens := map[string]bool{c.JobToken: true, c.KickToken: true}
	instances := map[string]bool{}
	for _, g := range c.Comets {
		if g.Identity == nil || g.Identity.InstanceId == "" || len(g.Identity.InstanceId) > 64 || !token(g.Token) || tokens[g.Token] || instances[g.Identity.InstanceId] || !loopback(g.RPCAddress, false) {
			return errConfig
		}
		for _, r := range g.Identity.InstanceId {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
				return errConfig
			}
		}
		id, err := uuid.Parse(g.Identity.BootId)
		if err != nil || id == uuid.Nil || len(g.Identity.BootId) != 36 {
			return errConfig
		}
		tokens[g.Token] = true
		instances[g.Identity.InstanceId] = true
	}
	publisher, err := kafkapush.New(c.Base.KafkaConfig.Brokers, c.Base.KafkaConfig.Topic)
	if err != nil {
		return errConfig
	}
	_ = publisher.Close()
	return nil
}
