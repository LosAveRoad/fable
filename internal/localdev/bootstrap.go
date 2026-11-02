// Package localdev prepares explicitly isolated local infrastructure. It is not
// a business service and is never used on the old application's database.
package localdev

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"regexp"
	"strconv"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
	logicapp "mychat/internal/logic/app"
)

var databaseName = regexp.MustCompile(`^fable_(local|e2e)_[a-zA-Z0-9_]{1,48}$`)
var topicName = regexp.MustCompile(`^fable\.push\.(v1|dlq\.v1)(\.[a-zA-Z0-9_-]+)?$`)
var dlqName = regexp.MustCompile(`^fable\.push\.dlq\.v1(\.[a-zA-Z0-9_-]+)?$`)
var errPrepare = errors.New("local infrastructure preparation failed; verify the selected isolated services")

func ensureDatabase(ctx context.Context, db *sql.DB, name string) (bool, error) {
	if !databaseName.MatchString(name) {
		return false, errors.New("local database name must use fable_local_ or fable_e2e_ prefix")
	}
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+name+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"); err != nil {
		return false, errPrepare
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?", name).Scan(&count); err != nil {
		return false, errPrepare
	}
	return count == 0, nil
}

// Prepare creates only the explicitly named local DB/topics. Existing populated
// tables are left untouched; Logic's normal startup verifies their schema.
func Prepare(ctx context.Context, c logicapp.Config, dlq string) error {
	if !databaseName.MatchString(c.Base.MySQLConfig.DatabaseName) || !topicName.MatchString(c.Base.KafkaConfig.Topic) || !dlqName.MatchString(dlq) || c.Base.KafkaConfig.Topic == dlq {
		return errPrepare
	}
	cfg := mysqlDriver.NewConfig()
	cfg.User = c.Base.MySQLConfig.User
	cfg.Passwd = c.Base.MySQLConfig.Password
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(c.Base.MySQLConfig.Host, strconv.Itoa(c.Base.MySQLConfig.Port))
	cfg.Timeout = 2 * time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return errPrepare
	}
	defer db.Close()
	empty, err := ensureDatabase(ctx, db, c.Base.MySQLConfig.DatabaseName)
	if err != nil {
		return err
	}
	if empty {
		if err := logicapp.InitSchema(ctx, c); err != nil {
			return err
		}
	}
	transport := &kafka.Transport{DialTimeout: 2 * time.Second}
	defer transport.CloseIdleConnections()
	client := &kafka.Client{Addr: kafka.TCP(c.Base.KafkaConfig.Brokers...), Transport: transport, Timeout: 5 * time.Second}
	for _, topic := range []string{c.Base.KafkaConfig.Topic, dlq} {
		configs := []kafka.ConfigEntry{}
		if topic == dlq {
			configs = append(configs, kafka.ConfigEntry{ConfigName: "retention.ms", ConfigValue: "604800000"}, kafka.ConfigEntry{ConfigName: "cleanup.policy", ConfigValue: "delete"})
		}
		reply, e := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 6, ReplicationFactor: 1, ConfigEntries: configs}}})
		if e != nil {
			return errPrepare
		}
		if e = reply.Errors[topic]; e != nil && !errors.Is(e, kafka.TopicAlreadyExists) {
			return errPrepare
		}
		for {
			metadata, e := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
			if e == nil && len(metadata.Topics) == 1 && metadata.Topics[0].Error == nil && len(metadata.Topics[0].Partitions) > 0 && len(metadata.Topics[0].Partitions) != 6 {
				return errPrepare
			}
			ready := e == nil && len(metadata.Topics) == 1 && metadata.Topics[0].Error == nil && len(metadata.Topics[0].Partitions) == 6
			if ready {
				reqs := make([]kafka.OffsetRequest, 6)
				for i := range reqs {
					reqs[i] = kafka.LastOffsetOf(i)
				}
				offsets, e := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: reqs}})
				ready = e == nil && len(offsets.Topics[topic]) == 6
				if ready {
					for _, partition := range offsets.Topics[topic] {
						if partition.Error != nil {
							ready = false
						}
					}
				}
			}
			if ready {
				break
			}
			select {
			case <-ctx.Done():
				return errPrepare
			case <-time.After(100 * time.Millisecond):
			}
		}
		if topic == dlq {
			description, e := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{Resources: []kafka.DescribeConfigRequestResource{{ResourceType: kafka.ResourceTypeTopic, ResourceName: dlq, ConfigNames: []string{"retention.ms", "cleanup.policy"}}}})
			if e != nil || len(description.Resources) != 1 || description.Resources[0].Error != nil {
				return errPrepare
			}
			values := map[string]string{}
			for _, entry := range description.Resources[0].ConfigEntries {
				values[entry.ConfigName] = entry.ConfigValue
			}
			if values["retention.ms"] != "604800000" || values["cleanup.policy"] != "delete" {
				return errors.New("existing DLQ must retain seven days with delete cleanup; refusing to alter it")
			}
		}
	}
	return nil
}
