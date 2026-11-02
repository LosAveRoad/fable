package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigEnvContractAndLegacyRejection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.toml")
	if err := os.WriteFile(path, []byte("[job]\nhttpAddress='127.0.0.1:0'\nlogicAddress='127.0.0.1:19001'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FABLE_JOB_TOKEN", strings.Repeat("j", 32))
	t.Setenv("FABLE_JOB_GROUP", "fable-push-job-v1.test")
	t.Setenv("FABLE_KAFKA_BROKERS", "127.0.0.1:39092,localhost:39093")
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Topic != "fable.push.v1" || c.DLQTopic != "fable.push.dlq.v1" || c.Group != "fable-push-job-v1.test" || len(c.Brokers) != 2 {
		t.Fatalf("config mismatch %+v", c)
	}
	t.Setenv("FABLE_KAFKA_TOPIC", "fable.chat.message")
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("accepted legacy command topic")
	}
}
func TestConfigRejectsUnsafeEndpointsAndCredentials(t *testing.T) {
	good := Config{HTTPAddress: "127.0.0.1:0", LogicAddress: "127.0.0.1:19001", JobToken: strings.Repeat("j", 32), Brokers: []string{"127.0.0.1:39092"}, Topic: "fable.push.v1", DLQTopic: "fable.push.dlq.v1", Group: "fable-push-job-v1"}
	if err := validate(good); err != nil {
		t.Fatal(err)
	}
	for _, f := range []func(*Config){func(c *Config) { c.HTTPAddress = "0.0.0.0:18080" }, func(c *Config) { c.LogicAddress = "remote.example:1" }, func(c *Config) { c.JobToken = "secret" }, func(c *Config) { c.Brokers = []string{"kafka://localhost:9092"} }, func(c *Config) { c.DLQTopic = c.Topic }, func(c *Config) { c.Group = "legacy" }} {
		c := good
		f(&c)
		if err := validate(c); err == nil {
			t.Fatal("unsafe config accepted")
		}
	}
}
