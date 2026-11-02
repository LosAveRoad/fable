// Package app owns the standalone Job consumer, connections, probes and replay.
package app

import (
	"errors"
	"github.com/BurntSushi/toml"
	"net"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddress, LogicAddress, JobToken string
	Brokers                             []string
	Topic, Group, DLQTopic              string
}

var errConfig = errors.New("invalid Job configuration; check endpoints, topic names and role credential")

func LoadConfig(path string) (Config, error) {
	var f struct {
		Job struct {
			HTTPAddress  string `toml:"httpAddress"`
			LogicAddress string `toml:"logicAddress"`
			Token        string `toml:"token"`
			Group        string `toml:"group"`
			DLQTopic     string `toml:"dlqTopic"`
		}
		Kafka struct {
			Brokers []string `toml:"brokers"`
			Topic   string   `toml:"topic"`
		}
	}
	if _, err := toml.DecodeFile(path, &f); err != nil {
		return Config{}, errConfig
	}
	c := Config{HTTPAddress: f.Job.HTTPAddress, LogicAddress: f.Job.LogicAddress, JobToken: f.Job.Token, Brokers: f.Kafka.Brokers, Topic: f.Kafka.Topic, Group: f.Job.Group, DLQTopic: f.Job.DLQTopic}
	if c.HTTPAddress == "" {
		c.HTTPAddress = "127.0.0.1:18082"
	}
	if c.LogicAddress == "" {
		c.LogicAddress = "127.0.0.1:19001"
	}
	if c.Topic == "" {
		c.Topic = "fable.push.v1"
	}
	if c.Group == "" {
		c.Group = "fable-push-job-v1"
	}
	if c.DLQTopic == "" {
		c.DLQTopic = "fable.push.dlq.v1"
	}
	if len(c.Brokers) == 0 {
		c.Brokers = []string{"127.0.0.1:9092"}
	}
	overlay := func(n string, p *string) {
		if s, ok := os.LookupEnv(n); ok {
			*p = s
		}
	}
	overlay("FABLE_JOB_HTTP_ADDR", &c.HTTPAddress)
	overlay("FABLE_JOB_LOGIC_ADDR", &c.LogicAddress)
	overlay("FABLE_JOB_TOKEN", &c.JobToken)
	overlay("FABLE_KAFKA_TOPIC", &c.Topic)
	overlay("FABLE_JOB_GROUP", &c.Group)
	overlay("FABLE_JOB_DLQ_TOPIC", &c.DLQTopic)
	if s, ok := os.LookupEnv("FABLE_KAFKA_BROKERS"); ok {
		c.Brokers = strings.Split(s, ",")
		for i := range c.Brokers {
			c.Brokers[i] = strings.TrimSpace(c.Brokers[i])
		}
	}
	if err := validate(c); err != nil {
		return Config{}, err
	}
	return c, nil
}
func loopback(address string, zero bool) bool {
	h, p, e := net.SplitHostPort(address)
	n, x := strconv.Atoi(p)
	ip := net.ParseIP(h)
	return e == nil && x == nil && n >= 0 && n <= 65535 && (zero || n > 0) && (h == "localhost" || ip != nil && ip.IsLoopback())
}
func named(s, prefix string) bool {
	if len(s) > 249 || s != prefix && (!strings.HasPrefix(s, prefix+".") || len(s) == len(prefix)+1) {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}
func validate(c Config) error {
	if !loopback(c.HTTPAddress, true) || !loopback(c.LogicAddress, false) || len(c.JobToken) < 32 || len(c.JobToken) > 256 || len(c.Brokers) == 0 || !named(c.Topic, "fable.push.v1") || !named(c.DLQTopic, "fable.push.dlq.v1") || !named(c.Group, "fable-push-job-v1") {
		return errConfig
	}
	for _, r := range c.JobToken {
		if r <= 32 || r > 126 {
			return errConfig
		}
	}
	for _, b := range c.Brokers {
		h, p, err := net.SplitHostPort(b)
		n, e := strconv.Atoi(p)
		if err != nil || e != nil || h == "" || n < 1 || n > 65535 || strconv.Itoa(n) != p || strings.ContainsAny(b, "/@?# \t\r\n\\") {
			return errConfig
		}
	}
	return nil
}
