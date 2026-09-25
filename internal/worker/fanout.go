package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	rediscache "kirfeed/internal/infra/cache"
	"kirfeed/internal/infra/mq"
	"kirfeed/internal/social"
	"kirfeed/internal/video"
	"log"
	"math/rand/v2"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type FanoutWorker struct {
	ch     *amqp.Channel
	videos *video.VideoRepository
	social *social.SocialRepository
	cache  *rediscache.Client
	queue  string
}

func NewFanoutWorker(ch *amqp.Channel, videos *video.VideoRepository, queue string) *FanoutWorker {
	return &FanoutWorker{
		ch:     ch,
		videos: videos,
		queue:  queue,
	}
}

const FanoutThreshold = 1000

func (w *FanoutWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.videos == nil {
		return errors.New("fanout worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}

	deliveries, err := w.ch.Consume(
		w.queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("deliveries channel closed")
			}
			w.handleDelivery(ctx, d)
		}
	}
}

func (w *FanoutWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	const maxRetries = 3
	for i := 0; i <= maxRetries; i++ {
		select {
		case <-ctx.Done():
			_ = d.Nack(false, true)
			return
		default:
		}
		if err := w.process(ctx, d.Body); err != nil {
			if i >= maxRetries {
				log.Printf("fanout worker: 重试 %d 次后仍失败, 丢弃: %v", maxRetries, err)
				_ = d.Nack(false, false)
				return
			}
			wait := time.Duration(1<<uint(i)) * time.Second
			log.Printf("fanout worker: 处理失败, %v 后重试 (%d/%d): %v", wait, i+1, maxRetries, err)
			time.Sleep(wait)
			continue
		}
		_ = d.Ack(false)
		return
	}
}

func (w *FanoutWorker) process(ctx context.Context, body []byte) error {
	var evt mq.FanoutEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil
	}
	if evt.AuthorID == 0 || evt.VideoID == 0 {
		return nil
	}

	followerCount, err := w.social.CountFollowers(ctx, evt.AuthorID)
	if err != nil {
		return err
	}
	if followerCount >= FanoutThreshold {

	}

	return nil
}

func (w *FanoutWorker) addAuthorOutbox(ctx context.Context, event mq.FanoutEvent) error {
	if w.cache == nil {
		return errors.New("cache is nil")
	}

	outboxkey := w.cache.Key("feed:author_outbox:%d", event.AuthorID)
	if err := w.cache.ZAdd(ctx, outboxkey, redis.Z{
		Score:  float64(event.CreateTime),
		Member: fmt.Sprintf("%d", event.VideoID),
	}); err != nil {
		return err
	}
	return w.cache.ZRemRangeByRank(ctx, outboxkey, 0, -501)
}

func (w *FanoutWorker) addFollowerInbox(ctx context.Context, event mq.FanoutEvent) error {
	if w.cache == nil || w.social == nil {
		return errors.New("cache or social repository is nil")
	}

	const batchSize = 500
	var cursor uint

	for {
		followerIDs, nextCursor, hasMore, err :=
			w.social.ListFollowerIDs(ctx, event.AuthorID, batchSize, cursor)
		if err != nil {
			return err
		}
		pipe := w.cache.Pipeline()
		for _, fid := range followerIDs {

			inboxkey := fmt.Sprintf("feed:follower_inbox:%d", fid)
			pipe.ZAdd(ctx, inboxkey, redis.Z{
				Score:  float64(event.CreateTime),
				Member: fmt.Sprintf("%d", event.VideoID),
			})
			if rand.Float64() < 0.01 { // 1% 概率裁剪
				pipe.ZRemRangeByRank(ctx, inboxkey, 0, -501)
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}

		if !hasMore {
			return nil
		}
		cursor = nextCursor
	}
}
