package main

import (
	"context"
	"fmt"
	"kirfeed/internal/config"
	apphttp "kirfeed/internal/http"
	rediscache "kirfeed/internal/infra/cache"
	"kirfeed/internal/infra/db"
	"kirfeed/internal/infra/mq"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

func run() error {
	// 加载 .env（本地开发）
	if err := godotenv.Load(); err != nil {
		log.Println(".env not found; continuing")
	}

	// 加载配置
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/config.yaml"
	}
	log.Printf("Loading config from %s", configPath)
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return err
	}

	// 连接数据库
	sqlDB, err := db.NewDB(cfg.Database)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	if err := db.AutoMigrate(sqlDB); err != nil {
		return fmt.Errorf("auto migrate database: %w", err)
	}
	defer db.CloseDB(sqlDB)

	// 连接 Redis (可选，用于缓存)
	cache, err := rediscache.NewFromEnv(&cfg.Redis)
	if err != nil {
		log.Printf("Redis config error (rediscache disabled): %v", err)
		cache = nil
	} else {
		pingCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if err := cache.Ping(pingCtx); err != nil {
			log.Printf("Redis not available (rediscache disabled): %v", err)
			_ = cache.Close()
			cache = nil
		} else {
			defer cache.Close()
			log.Printf("Redis connected (rediscache enabled)")
		}
	}

	// 连接 RabbitMQ (可选，用于消息队列)
	rmq, err := mq.NewRabbitMQ(&cfg.RabbitMQ)
	if err != nil {
		log.Printf("RabbitMQ config error (disabled): %v", err)
		rmq = nil
	} else {
		defer rmq.Close()
		log.Printf("RabbitMQ connected")
	}

	// 设置路由
	r := apphttp.SetRouter(sqlDB, cache, rmq)
	log.Printf("Server is running on port %d", cfg.Server.Port)
	if err := r.Run(":" + strconv.Itoa(cfg.Server.Port)); err != nil {
		return fmt.Errorf("run server: %w", err)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error occurred: %v", err)
	}
}
