package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validConfig() Config {
	return Config{InstanceID: "comet-test", BootID: "11111111-1111-4111-8111-111111111111", HTTPAddress: "127.0.0.1:8082", RPCAddress: "127.0.0.1:9092", LogicAddress: "127.0.0.1:9091", LogicToken: strings.Repeat("c", 32), JobToken: strings.Repeat("j", 32), KickToken: strings.Repeat("k", 32), AdminToken: strings.Repeat("a", 32), JWTSecret: strings.Repeat("s", 32), AllowedOrigins: []string{"http://localhost:5173"}}
}
func TestRunRejectsUnsafeConfiguration(t *testing.T) {
	for _, mutate := range []func(*Config){func(c *Config) { c.BootID = "" }, func(c *Config) { c.HTTPAddress = "0.0.0.0:8082" }, func(c *Config) { c.LogicAddress = "remote.example:9091" }, func(c *Config) { c.JobToken = c.KickToken }, func(c *Config) { c.AllowedOrigins = []string{"*"} }, func(c *Config) { c.JWTSecret = "" }} {
		c := validConfig()
		mutate(&c)
		if e := Run(context.Background(), c); e == nil {
			t.Fatal("accepted unsafe config")
		}
	}
}
func TestLoadConfigUsesExplicitIdentityAndEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "comet.toml")
	if e := os.WriteFile(path, []byte("[comet]\nhttpAddress='127.0.0.1:8082'\nrpcAddress='127.0.0.1:9092'\nlogicAddress='127.0.0.1:9091'\n"), 0600); e != nil {
		t.Fatal(e)
	}
	c := validConfig()
	for k, v := range map[string]string{"FABLE_COMET_INSTANCE_ID": c.InstanceID, "FABLE_COMET_BOOT_ID": c.BootID, "FABLE_COMET_LOGIC_TOKEN": c.LogicToken, "FABLE_COMET_JOB_TOKEN": c.JobToken, "FABLE_COMET_KICK_TOKEN": c.KickToken, "FABLE_COMET_ADMIN_TOKEN": c.AdminToken, "FABLE_JWT_SECRET": c.JWTSecret, "FABLE_COMET_ALLOWED_ORIGINS": "http://localhost:5173,http://127.0.0.1:5173"} {
		t.Setenv(k, v)
	}
	got, e := LoadConfig(path)
	if e != nil {
		t.Fatal(e)
	}
	if got.BootID != c.BootID || len(got.AllowedOrigins) != 2 || got.RPCAddress != c.RPCAddress {
		t.Fatalf("wrong overlays %+v", got)
	}
	t.Setenv("FABLE_COMET_BOOT_ID", "")
	if _, e = LoadConfig(path); e == nil {
		t.Fatal("defaulted missing boot")
	}
}
