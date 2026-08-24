package main

import (
	"context"
	"kirfeed/internal/config"
	rediscache "kirfeed/internal/infra/cache"
	"kirfeed/internal/infra/db"
	"kirfeed/internal/infra/mq"
	"kirfeed/internal/worker"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/gorm"
)

func connectWithRetry(name string, maxRetries int, fn func() error) {
	for i := 0; i < maxRetries; i++ {
		if err := fn(); err == nil {
			return
		}
		wait := time.Duration(1<<i) * time.Second
		if wait > 30*time.Second {
			wait = 30 * time.Second
		}
		log.Printf("%s 不可用，%v 后重试 (%d/%d)...", name, wait, i+1, maxRetries)
		time.Sleep(wait)
	}
	log.Fatalf("%s: 超过最大重试次数", name)
}

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

	// 连接数据库（带重试）
	var sqlDB *gorm.DB
	connectWithRetry("MySQL", 10, func() error {
		var err error
		sqlDB, err = db.NewDB(cfg.Database)
		return err
	})
	defer db.CloseDB(sqlDB)

	// 连接 Redis（用于流行度更新）
	cache, err := rediscache.NewFromEnv(&cfg.Redis)
	if err != nil {
		log.Printf("Redis config error (popularity worker disabled): %v", err)
		cache = nil
	} else {
		pingCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if err := cache.Ping(pingCtx); err != nil {
			log.Printf("Redis not available (popularity worker disabled): %v", err)
			_ = cache.Close()
			cache = nil
		} else {
			defer cache.Close()
			log.Printf("Redis connected (popularity worker enabled)")
		}
	}
	// 连接 RabbitMQ（带重试）
	url := "amqp://" + cfg.RabbitMQ.Username + ":" + cfg.RabbitMQ.Password + "@" + cfg.RabbitMQ.Host + ":" + strconv.Itoa(cfg.RabbitMQ.Port) + "/"
	var conn *amqp.Connection
	connectWithRetry("RabbitMQ", 10, func() error {
		var err error
		conn, err = amqp.Dial(url)
		return err
	})
	defer conn.Close()

	rmq := &mq.RabbitMQ{Conn: conn}
	timelineMQ, err := mq.NewTimelineMQ(rmq)
	if err != nil {
		return err
	}
	worker.StartOutboxPoller(sqlDB, timelineMQ)
	worker.StartConsumer(timelineMQ, "video.timeline.update.queue", cache, rmq)
	log.Println("Video timeline worker started")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error occurred: %v", err)
	}
}
