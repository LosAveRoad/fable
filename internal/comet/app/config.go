// Package app wires the standalone Comet without business backend imports.
package app

import (
	"errors"
	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	InstanceID     string   `toml:"instanceID"`
	BootID         string   `toml:"bootID"`
	HTTPAddress    string   `toml:"httpAddress"`
	RPCAddress     string   `toml:"rpcAddress"`
	LogicAddress   string   `toml:"logicAddress"`
	LogicToken     string   `toml:"logicToken"`
	JobToken       string   `toml:"jobToken"`
	KickToken      string   `toml:"kickToken"`
	AdminToken     string   `toml:"adminToken"`
	JWTSecret      string   `toml:"jwtSecret"`
	AllowedOrigins []string `toml:"allowedOrigins"`
	QueueSize      int      `toml:"queueSize"`
}

var errConfig = errors.New("invalid Comet configuration; check explicit identity, loopback addresses and role credentials")

func LoadConfig(path string) (Config, error) {
	var file struct {
		Comet Config `toml:"comet"`
	}
	file.Comet = Config{HTTPAddress: "127.0.0.1:8082", RPCAddress: "127.0.0.1:9092", LogicAddress: "127.0.0.1:9091", QueueSize: 256}
	if _, e := toml.DecodeFile(path, &file); e != nil {
		return Config{}, errors.New("cannot load Comet configuration")
	}
	c := file.Comet
	for name, dst := range map[string]*string{"FABLE_COMET_INSTANCE_ID": &c.InstanceID, "FABLE_COMET_BOOT_ID": &c.BootID, "FABLE_COMET_HTTP_ADDR": &c.HTTPAddress, "FABLE_COMET_RPC_ADDR": &c.RPCAddress, "FABLE_COMET_LOGIC_ADDR": &c.LogicAddress, "FABLE_COMET_LOGIC_TOKEN": &c.LogicToken, "FABLE_COMET_JOB_TOKEN": &c.JobToken, "FABLE_COMET_KICK_TOKEN": &c.KickToken, "FABLE_COMET_ADMIN_TOKEN": &c.AdminToken, "FABLE_JWT_SECRET": &c.JWTSecret} {
		if v, ok := os.LookupEnv(name); ok {
			*dst = v
		}
	}
	if v, ok := os.LookupEnv("FABLE_COMET_ALLOWED_ORIGINS"); ok {
		c.AllowedOrigins = strings.Split(v, ",")
		for i := range c.AllowedOrigins {
			c.AllowedOrigins[i] = strings.TrimSpace(c.AllowedOrigins[i])
		}
	}
	if e := validate(c); e != nil {
		return Config{}, e
	}
	return c, nil
}
func loopback(address string) bool {
	host, p, e := net.SplitHostPort(address)
	port, pe := strconv.Atoi(p)
	ip := net.ParseIP(host)
	return e == nil && pe == nil && port > 0 && port <= 65535 && (host == "localhost" || ip != nil && ip.IsLoopback())
}
func token(s string) bool {
	if len(s) < 32 || len(s) > 256 {
		return false
	}
	for _, r := range s {
		if r <= 32 || r > 126 {
			return false
		}
	}
	return true
}
func validate(c Config) error {
	if len(c.InstanceID) == 0 || len(c.InstanceID) > 64 || !loopback(c.HTTPAddress) || !loopback(c.RPCAddress) || !loopback(c.LogicAddress) || c.HTTPAddress == c.RPCAddress || len(c.JWTSecret) < 32 || c.QueueSize < 0 || c.QueueSize > 65536 || len(c.AllowedOrigins) == 0 {
		return errConfig
	}
	for _, r := range c.InstanceID {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			return errConfig
		}
	}
	id, e := uuid.Parse(c.BootID)
	if e != nil || id == uuid.Nil || len(c.BootID) != 36 {
		return errConfig
	}
	seen := map[string]bool{}
	for _, s := range []string{c.LogicToken, c.JobToken, c.KickToken, c.AdminToken} {
		if !token(s) || seen[s] {
			return errConfig
		}
		seen[s] = true
	}
	for _, origin := range c.AllowedOrigins {
		u, e := url.Parse(origin)
		if e != nil || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
			return errConfig
		}
	}
	return nil
}
