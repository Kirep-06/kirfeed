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

const followBackfillLimit = 500

type SocialWorker struct {
	ch        *amqp.Channel
	repo      *social.SocialRepository
	videos    *video.VideoRepository
	feedCache *feed.FeedCache
	queue     string
}

func NewSocialWorker(ch *amqp.Channel, repo *social.SocialRepository, videos *video.VideoRepository, feedCache *feed.FeedCache, queue string) *SocialWorker {
	return &SocialWorker{ch: ch, repo: repo, videos: videos, feedCache: feedCache, queue: queue}
}

func (w *SocialWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.repo == nil {
		return errors.New("social worker is not initialized")
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

func (w *SocialWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
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
				log.Printf("social worker: 重试 %d 次后仍失败, 发送到死信队列: %v", maxRetries, err)
				_ = d.Nack(false, false)
				return
			}
			wait := time.Duration(1<<uint(i)) * time.Second
			log.Printf("social worker: 处理失败, %v 后重试 (%d/%d): %v", wait, i+1, maxRetries, err)
			time.Sleep(wait)
			continue
		}
		_ = d.Ack(false)
		return
	}
}

// process handles feed-index side effects only. The relation itself has already
// been committed by SocialService before its event is published.
func (w *SocialWorker) process(ctx context.Context, body []byte) error {
	var event mq.SocialEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return nil
	}
	if event.FollowerID == 0 || event.FolloweeID == 0 || w.feedCache == nil || w.videos == nil {
		return nil
	}
	relation := &social.Social{FollowerID: event.FollowerID, FolloweeID: event.FolloweeID}
	isFollowed, err := w.repo.IsFollowed(ctx, relation)
	if err != nil {
		return err
	}

	switch event.Action {
	case "follow":
		if !isFollowed { // Ignore a stale event if the user has already unfollowed.
			return nil
		}
		followerCount, err := w.repo.CountFollowers(ctx, event.FolloweeID)
		if err != nil {
			return err
		}
		if followerCount >= FanoutThreshold {
			return nil
		}
		videos, err := w.videos.ListRecentByAuthorID(ctx, event.FolloweeID, followBackfillLimit)
		if err != nil {
			return err
		}
		return w.feedCache.FollowAuthor(ctx, event.FollowerID, event.FolloweeID, false, fanoutEvents(event.FolloweeID, videos))
	case "unfollow":
		if isFollowed { // Ignore a stale event if the user has followed again.
			return nil
		}
		videos, err := w.videos.ListRecentByAuthorID(ctx, event.FolloweeID, followBackfillLimit)
		if err != nil {
			return err
		}
		return w.feedCache.RemoveFollow(ctx, event.FollowerID, event.FolloweeID, fanoutEvents(event.FolloweeID, videos))
	default:
		return nil
	}
}

func fanoutEvents(authorID uint, videos []video.Video) []mq.FanoutEvent {
	events := make([]mq.FanoutEvent, 0, len(videos))
	for _, item := range videos {
		events = append(events, mq.FanoutEvent{
			VideoID:    item.ID,
			AuthorID:   authorID,
			CreateTime: item.CreateTime.UnixMilli(),
		})
	}
	return events
}
