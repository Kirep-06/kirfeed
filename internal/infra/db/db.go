package db

import (
	"fmt"
	"kirfeed/internal/account"
	"kirfeed/internal/config"
	"kirfeed/internal/social"
	"kirfeed/internal/video"
	"os"
	"strconv"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func NewDB(dbcfg config.DatabaseConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		dbcfg.User, dbcfg.Password, dbcfg.Host, dbcfg.Port, dbcfg.DBName)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	if raw := os.Getenv("DB_MAX_OPEN_CONNS"); raw != "" {
		maxOpen, parseErr := strconv.Atoi(raw)
		if parseErr != nil || maxOpen < 1 {
			return nil, fmt.Errorf("invalid DB_MAX_OPEN_CONNS: %q", raw)
		}
		sqlDB, err := db.DB()
		if err != nil {
			return nil, err
		}
		sqlDB.SetMaxOpenConns(maxOpen)
		sqlDB.SetMaxIdleConns(maxOpen / 2)
		sqlDB.SetConnMaxLifetime(30 * time.Minute)
	}

	return db, nil
}

func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&account.Account{},
		&video.OutboxMsg{},
		&video.Video{},
		&video.Like{},
		&video.Comment{},
		&social.Social{},
	)
}

func CloseDB(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
