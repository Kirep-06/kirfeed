package main

import (
	"context"
	"kirfeed/internal/config"
	rediscache "kirfeed/internal/infra/cache"
	"kirfeed/internal/infra/db"
	"kirfeed/internal/infra/mq"
	"kirfeed/internal/social"
	"kirfeed/internal/video"
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

const (
	socialExchange   = "social.events"
	socialQueue      = "social.events"
	socialBindingKey = "social.*"

	likeExchange   = "like.events"
	likeQueue      = "like.events"
	likeBindingKey = "like.*"

	commentExchange   = "comment.events"
	commentQueue      = "comment.events"
	commentBindingKey = "comment.*"

	popularityExchange   = "video.popularity.events"
	popularityQueue      = "video.popularity.events"
	popularityBindingKey = "video.popularity.*"
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

// runWorkerWithRetry 为每个 Worker 创建独立 Channel，断开后自动重连
func runWorkerWithRetry(ctx context.Context, name string, conn *amqp.Connection, fn func(*amqp.Channel) error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		ch, err := conn.Channel()
		if err != nil {
			log.Printf("%s: 创建 Channel 失败: %v, 5秒后重试", name, err)
			time.Sleep(5 * time.Second)
			continue
		}
		if err := ch.Qos(50, 0, false); err != nil {
			log.Printf("%s: QoS 设置失败: %v", name, err)
		}

		log.Printf("%s started, consuming", name)
		if err := fn(ch); err != nil {
			if ctx.Err() != nil {
				ch.Close()
				return
			}
			log.Printf("%s: %v, 5秒后重连...", name, err)
		}
		ch.Close()
		time.Sleep(5 * time.Second)
	}
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

	// 用临时 Channel 声明拓扑（持久化队列，声明一次即可）
	topoCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open topology channel: %v", err)
	}
	if err := declareLikeTopology(topoCh); err != nil {
		log.Fatalf("Failed to declare like topology: %v", err)
	}
	if err := mq.DeclareTopic(topoCh, socialExchange, socialQueue, socialBindingKey); err != nil {
		log.Fatalf("Failed to declare social topology: %v", err)
	}
	if err := mq.DeclareTopic(topoCh, commentExchange, commentQueue, commentBindingKey); err != nil {
		log.Fatalf("Failed to declare comment topology: %v", err)
	}
	if cache != nil {
		if err := declarePopularityTopology(topoCh); err != nil {
			log.Fatalf("Failed to declare popularity topology: %v", err)
		}
	}
	topoCh.Close()

	// 准备 repo
	videoRepo := video.NewVideoRepository(sqlDB)
	likeRepo := video.NewLikeRepository(sqlDB)
	commentRepo := video.NewCommentRepository(sqlDB)
	socialRepo := social.NewSocialRepository(sqlDB)

	rmq := &mq.RabbitMQ{Conn: conn}
	timelineMQ, err := mq.NewTimelineMQ(rmq)
	if err != nil {
		return err
	}
	fanoutMQ, err := mq.NewFanoutMQ(rmq)
	if err != nil {
		return err
	}
	defer fanoutMQ.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 每个 Worker 独立 Channel + 自动重连
	go runWorkerWithRetry(ctx, "LikeWorker", conn, func(ch *amqp.Channel) error {
		return worker.NewLikeWorker(ch, likeRepo, videoRepo, likeQueue).Run(ctx)
	})
	go runWorkerWithRetry(ctx, "CommentWorker", conn, func(ch *amqp.Channel) error {
		return worker.NewCommentWorker(ch, commentRepo, videoRepo, commentQueue).Run(ctx)
	})
	go runWorkerWithRetry(ctx, "SocialWorker", conn, func(ch *amqp.Channel) error {
		return worker.NewSocialWorker(ch, socialRepo, socialQueue).Run(ctx)
	})
	if cache != nil {
		go runWorkerWithRetry(ctx, "PopularityWorker", conn, func(ch *amqp.Channel) error {
			return worker.NewPopularityWorker(ch, cache, popularityQueue).Run(ctx)
		})
		go runWorkerWithRetry(ctx, "FanoutWorker", conn, func(ch *amqp.Channel) error {
			return worker.NewFanoutWorker(ch, videoRepo, socialRepo, cache, "video.fanout.queue").Run(ctx)
		})
	}

	worker.StartOutboxPoller(sqlDB, timelineMQ, fanoutMQ)
	worker.StartConsumer(timelineMQ, "video.timeline.update.queue", cache, rmq)
	log.Println("Video timeline worker started")

	defer stop()
	<-ctx.Done()
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error occurred: %v", err)
	}
}

func declarePopularityTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(
		popularityExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		popularityQueue,
		true,
		false,
		false,
		false,
		amqp.Table{"x-dead-letter-exchange": mq.DLXExchange},
	)
	if err != nil {
		return err
	}

	return ch.QueueBind(
		q.Name,
		popularityBindingKey,
		popularityExchange,
		false,
		nil,
	)
}

func declareLikeTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(
		likeExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		likeQueue,
		true,
		false,
		false,
		false,
		amqp.Table{"x-dead-letter-exchange": mq.DLXExchange},
	)
	if err != nil {
		return err
	}

	return ch.QueueBind(
		q.Name,
		likeBindingKey,
		likeExchange,
		false,
		nil,
	)
}
