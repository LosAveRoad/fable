package dao

// 用作 gorm 配置

import (
	"errors"
	"fmt"

	"mychat/internal/config"
	"mychat/internal/model"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var GormDB *gorm.DB

// InitGorm 初始化 GORM 和 MySQL 连接。
func InitGorm(mysqlConfig config.MySQLConfig) error {
	if GormDB != nil {
		return errors.New("gorm has already been initialized")
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		mysqlConfig.User,
		mysqlConfig.Password,
		mysqlConfig.Host,
		mysqlConfig.Port,
		mysqlConfig.DatabaseName,
	)

	db, err := gorm.Open(
		mysql.Open(dsn),
		&gorm.Config{},
	)
	if err != nil {
		return fmt.Errorf("open mysql with gorm: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("get underlying sql db: %w", err)
	}

	// 确认数据库当前可以正常连接。
	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return fmt.Errorf("ping mysql: %w", err)
	}

	if err := db.AutoMigrate(
		&model.UserInfo{},
		&model.Session{},
		&model.Message{},
		&model.UserAISessionAccess{},
	); err != nil {
		_ = sqlDB.Close()
		return fmt.Errorf("auto migrate mysql tables: %w", err)
	}

	GormDB = db

	return nil
}

// CloseGorm 关闭 GORM 底层的数据库连接。
func CloseGorm() error {
	if GormDB == nil {
		return nil
	}

	sqlDB, err := GormDB.DB()
	if err != nil {
		return fmt.Errorf("get underlying sql db: %w", err)
	}

	if err := sqlDB.Close(); err != nil {
		return fmt.Errorf("close mysql: %w", err)
	}

	GormDB = nil

	return nil
}
