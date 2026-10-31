package app

import (
	pb "mychat/api/gen/fable/v1"
	"mychat/internal/config"
	"mychat/internal/logic"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validConfig() Config {
	return Config{Base: config.Config{MySQLConfig: config.MySQLConfig{Host: "127.0.0.1", Port: 33316, User: "root", DatabaseName: "fable_logic_test_app"}, RedisConfig: config.RedisConfig{Enabled: true, Host: "127.0.0.1", Port: 36379}, KafkaConfig: config.KafkaConfig{Enabled: true, Mode: "kafka", Brokers: []string{"127.0.0.1:39092"}, Topic: "fable.push.v1"}, JWTConfig: config.JWTConfig{Secret: []byte(strings.Repeat("j", 32))}}, HTTPAddress: "127.0.0.1:8081", RPCAddress: "127.0.0.1:9091", RedisPrefix: "test:", JobToken: strings.Repeat("b", 32), KickToken: strings.Repeat("k", 32), Comets: []logic.CometGrant{{Identity: &pb.CometIdentity{InstanceId: "comet-dev", BootId: "11111111-1111-4111-8111-111111111111"}, RPCAddress: "127.0.0.1:9092", Token: strings.Repeat("c", 32)}}}
}
func TestRejectUnsafeConfiguration(t *testing.T) {
	for _, change := range []func(*Config){func(c *Config) { c.Base.RedisConfig.Enabled = false }, func(c *Config) { c.Base.KafkaConfig.Enabled = false }, func(c *Config) { c.Base.KafkaConfig.Mode = "local" }, func(c *Config) { c.HTTPAddress = "0.0.0.0:8080" }, func(c *Config) { c.RPCAddress = ":9090" }, func(c *Config) { c.Comets = nil }, func(c *Config) { c.Comets[0].Token = c.JobToken }, func(c *Config) { c.Base.MySQLConfig.DatabaseName = "" }, func(c *Config) { c.Base.MySQLConfig.DatabaseName = "mysql" }, func(c *Config) { c.Comets[0].RPCAddress = "remote.example:1234" }, func(c *Config) { c.RedisPrefix = "bad{slot}" }} {
		c := validConfig()
		change(&c)
		if err := validate(c); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
}
func TestLoadConfigOverlaysAndRedactsMalformedGrants(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logic.toml")
	err := os.WriteFile(path, []byte(`[mysqlConfig]
host="127.0.0.1"
port=33316
user="root"
databaseName="fable_logic_test_app"
[redisConfig]
enabled=true
host="127.0.0.1"
port=36379
[kafkaConfig]
enabled=true
mode="kafka"
brokers=["127.0.0.1:39092"]
topic="fable.push.v1"
[logic]
httpAddress="127.0.0.1:8081"
rpcAddress="127.0.0.1:9091"
redisPrefix="test:"
`), 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("FABLE_JWT_SECRET", strings.Repeat("j", 32))
	t.Setenv("FABLE_LOGIC_JOB_TOKEN", strings.Repeat("b", 32))
	t.Setenv("FABLE_LOGIC_KICK_TOKEN", strings.Repeat("k", 32))
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("missing grant accepted")
	}
	t.Setenv("FABLE_LOGIC_COMET_GRANTS_JSON", `[{"identity":{"instance_id":"comet-dev","boot_id":"11111111-1111-4111-8111-111111111111"},"rpc_address":"127.0.0.1:9092","token":"`+strings.Repeat("c", 32)+`"}]`)
	t.Setenv("FABLE_LOGIC_HTTP_ADDR", "127.0.0.1:8181")
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddress != "127.0.0.1:8181" || len(c.Comets) != 1 {
		t.Fatal("overlay missing")
	}
	t.Setenv("FABLE_REDIS_ENABLED", "malformed")
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("malformed boolean overlay accepted")
	}
	t.Setenv("FABLE_REDIS_ENABLED", "true")
	t.Setenv("FABLE_LOGIC_COMET_GRANTS_JSON", `secret-leak`)
	if _, err := LoadConfig(path); err == nil || strings.Contains(err.Error(), "secret-leak") {
		t.Fatal("malformed grants not safely rejected")
	}
}
