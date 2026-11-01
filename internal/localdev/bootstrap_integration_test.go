//go:build integration

package localdev

import (
	"context"
	"database/sql"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	logicapp "mychat/internal/logic/app"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPrepareRefusesChangedDLQRetentionAndPreservesExistingSchema(t *testing.T) {
	dsn, broker := os.Getenv("FABLE_LOGIC_TEST_MYSQL_DSN"), os.Getenv("FABLE_LOGIC_TEST_KAFKA_BROKER")
	if dsn == "" || broker == "" {
		t.Skip("isolated MySQL/Kafka required")
	}
	cfg, e := mysqlDriver.ParseDSN(dsn)
	if e != nil {
		t.Fatal(e)
	}
	cfg.DBName = ""
	admin, e := sql.Open("mysql", cfg.FormatDSN())
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	name := "fable_e2e_prepare_" + suffix
	defer admin.Exec("DROP DATABASE IF EXISTS `" + name + "`")
	c := logicapp.Config{}
	host, port, _ := net.SplitHostPort(cfg.Addr)
	c.Base.MySQLConfig.Host = host
	c.Base.MySQLConfig.Port, _ = strconv.Atoi(port)
	c.Base.MySQLConfig.User = cfg.User
	c.Base.MySQLConfig.Password = cfg.Passwd
	c.Base.MySQLConfig.DatabaseName = name
	c.Base.KafkaConfig.Brokers = []string{broker}
	c.Base.KafkaConfig.Topic = "fable.push.v1." + suffix
	dlq := "fable.push.dlq.v1." + suffix
	transport := &kafka.Transport{}
	defer transport.CloseIdleConnections()
	client := &kafka.Client{Addr: kafka.TCP(broker), Transport: transport, Timeout: 3 * time.Second}
	defer client.DeleteTopics(context.Background(), &kafka.DeleteTopicsRequest{Topics: []string{c.Base.KafkaConfig.Topic, dlq}})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if e := Prepare(ctx, c, dlq); e != nil {
		t.Fatal(e)
	}
	if e := Prepare(ctx, c, dlq); e != nil {
		t.Fatalf("second startup changed valid schema: %v", e)
	}
	response, e := client.AlterConfigs(ctx, &kafka.AlterConfigsRequest{Resources: []kafka.AlterConfigRequestResource{{ResourceType: kafka.ResourceTypeTopic, ResourceName: dlq, Configs: []kafka.AlterConfigRequestConfig{{Name: "retention.ms", Value: "60000"}}}}})
	if e != nil {
		t.Fatal(e)
	}
	for _, err := range response.Errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if e := Prepare(ctx, c, dlq); e == nil {
		t.Fatal("accepted shorter existing DLQ retention instead of refusing nonconforming configuration")
	}
}
