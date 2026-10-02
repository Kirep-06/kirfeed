package worker

import (
	"context"
	"encoding/json"
	"errors"
	"kirfeed/internal/feed"
	"kirfeed/internal/infra/mq"
	"kirfeed/internal/social"
	"kirfeed/internal/video"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type FanoutWorker struct {
	ch     *amqp.Channel
	videos *video.VideoRepository
	social *social.SocialRepository
	cache  *feed.FeedCache
	queue  string
}

func NewFanoutWorker(ch *amqp.Channel, videos *video.VideoRepository, socialRepo *social.SocialRepository, cache *feed.FeedCache, queue string) *FanoutWorker {
	return &FanoutWorker{ch: ch, videos: videos, social: socialRepo, cache: cache, queue: queue}
}

const FanoutThreshold = feed.BigCreatorFollowerThreshold

func (w *FanoutWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.videos == nil || w.social == nil || w.cache == nil {
		return errors.New("fanout worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}
	deliveries, err := w.ch.Consume(w.queue, "", false, false, false, false, nil)
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
	var event mq.FanoutEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return nil
	}
	if event.AuthorID == 0 || event.VideoID == 0 {
		return nil
	}
	followerCount, err := w.social.CountFollowers(ctx, event.AuthorID)
	if err != nil {
		return err
	}
	if followerCount >= FanoutThreshold {
		return w.cache.AddAuthorOutboxItem(ctx, event)
	}
	return w.addFollowerInbox(ctx, event)
}

func (w *FanoutWorker) addFollowerInbox(ctx context.Context, event mq.FanoutEvent) error {
	const batchSize = 100
	var cursor uint
	for {
		followerIDs, nextCursor, hasMore, err := w.social.ListFollowerIDs(ctx, event.AuthorID, batchSize, cursor)
		if err != nil {
			return err
		}
		if len(followerIDs) == 0 {
			return nil
		}
		if err := w.cache.AddFollowerInboxItems(ctx, event, followerIDs); err != nil {
			return err
		}
		if !hasMore {
			return nil
		}
		cursor = nextCursor
	}
}
