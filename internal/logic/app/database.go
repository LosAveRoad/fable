package app

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"strconv"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	mysqlGorm "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"mychat/internal/config"
	"mychat/internal/logic/messagestore"
)

var errDatabase = errors.New("Logic database unavailable")
var errSchema = errors.New("Logic target schema required; use init-schema only with an explicitly selected empty database")

func openDatabase(ctx context.Context, c config.MySQLConfig) (*gorm.DB, *sql.DB, error) {
	if !databaseValid(c) {
		return nil, nil, errConfig
	}
	connection := mysqlDriver.NewConfig()
	connection.User = c.User
	connection.Passwd = c.Password
	connection.Net = "tcp"
	connection.Addr = net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	connection.DBName = c.DatabaseName
	connection.ParseTime = true
	connection.Loc = time.UTC
	connection.Timeout = 2 * time.Second
	connection.ReadTimeout = 12 * time.Second
	connection.WriteTimeout = 12 * time.Second
	connection.Params = map[string]string{"charset": "utf8mb4"}
	raw, err := sql.Open("mysql", connection.FormatDSN())
	if err != nil {
		return nil, nil, errDatabase
	}
	raw.SetMaxOpenConns(20)
	raw.SetMaxIdleConns(5)
	raw.SetConnMaxLifetime(3 * time.Minute)
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := raw.PingContext(bounded); err != nil {
		raw.Close()
		return nil, nil, errDatabase
	}
	db, err := gorm.Open(mysqlGorm.New(mysqlGorm.Config{Conn: raw, SkipInitializeWithVersion: true}), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		raw.Close()
		return nil, nil, errDatabase
	}
	return db, raw, nil
}

// InitSchema never creates a database, drops a table, or backfills old rows. The
// operator must explicitly select an existing, completely empty database first.
func InitSchema(ctx context.Context, c Config) error {
	db, raw, err := openDatabase(ctx, c.Base.MySQLConfig)
	if err != nil {
		return err
	}
	defer raw.Close()
	return initialize(ctx, db)
}
func initialize(ctx context.Context, db *gorm.DB) error {
	var count int64
	if err := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()").Scan(&count).Error; err != nil {
		return errDatabase
	}
	if count != 0 {
		return errors.New("refusing init-schema: selected database is not empty")
	}
	if err := messagestore.InitSchema(ctx, db); err != nil {
		return errDatabase
	}
	return verifySchema(ctx, db)
}

// verifySchema is read-only. Serving never calls AutoMigrate or InitSchema.
func verifySchema(ctx context.Context, db *gorm.DB) error {
	var count int64
	if err := db.WithContext(ctx).Raw("SELECT COUNT(DISTINCT table_name) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN ('user_info','session','group_info','user_ai_session_access','user_contact','contact_apply','message') AND table_type = 'BASE TABLE'").Scan(&count).Error; err != nil {
		return errDatabase
	}
	if count != 7 {
		return errSchema
	}
	if err := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'message' AND is_nullable = 'NO' AND ((column_name = 'client_message_id' AND data_type = 'char' AND character_maximum_length = 36) OR (column_name = 'request_fingerprint' AND data_type = 'binary' AND character_maximum_length = 32) OR (column_name = 'recipient_ids' AND data_type = 'json'))").Scan(&count).Error; err != nil {
		return errDatabase
	}
	if count != 3 {
		return errSchema
	}
	if err := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM (SELECT index_name FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'message' AND non_unique = 0 GROUP BY index_name HAVING GROUP_CONCAT(column_name ORDER BY seq_in_index) = 'send_id,client_message_id' AND COUNT(*) = 2) AS sender_client_indexes").Scan(&count).Error; err != nil {
		return errDatabase
	}
	if count < 1 {
		return errSchema
	}
	return nil
}
