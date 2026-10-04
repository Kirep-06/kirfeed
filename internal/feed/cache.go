package feed

import (
	"context"
	"errors"
	"fmt"
	rediscache "kirfeed/internal/infra/cache"
	"kirfeed/internal/infra/mq"

	"github.com/redis/go-redis/v9"
)

const maxInboxItems = 500
const maxOutboxItems = 500

type FeedCache struct {
	cache *rediscache.Client
}

func NewFeedCache(cache *rediscache.Client) *FeedCache {
	if cache == nil {
		return nil
	}
	return &FeedCache{cache: cache}
}

// AddAuthorOutboxItem appends a published video to its author's shared outbox.
func (c *FeedCache) AddAuthorOutboxItem(ctx context.Context, event mq.FanoutEvent) error {
	if c == nil || c.cache == nil {
		return errors.New("cache is nil")
	}
	if event.AuthorID == 0 || event.VideoID == 0 {
		return errors.New("IDs must be positive")
	}
	pipe := c.cache.Pipeline()
	if pipe == nil {
		return errors.New("redis pipeline is nil")
	}
	outboxKey := c.cache.AuthorOutboxKey(event.AuthorID)
	pipe.ZAdd(ctx, outboxKey, redis.Z{
		Score:  float64(event.CreateTime),
		Member: feedVideoMember(event.AuthorID, event.VideoID),
	})

	// if rand.Float64() < 0.01 {
	// 	pipe.ZRemRangeByRank(ctx, outboxKey, 0, -int64(maxOutboxItems)-1)
	// }

	pipe.ZRemRangeByRank(ctx, outboxKey, 0, -int64(maxOutboxItems)-1)
	_, err := pipe.Exec(ctx)
	return err
}

// AddFollowerInboxItems pushes one newly published video to a page of followers.
func (c *FeedCache) AddFollowerInboxItems(ctx context.Context, event mq.FanoutEvent, followerIDs []uint) error {
	if c == nil || c.cache == nil {
		return errors.New("cache is nil")
	}
	if event.AuthorID == 0 || event.VideoID == 0 {
		return errors.New("IDs must be positive")
	}
	if len(followerIDs) == 0 {
		return nil
	}

	pipe := c.cache.Pipeline()
	if pipe == nil {
		return errors.New("redis pipeline is nil")
	}
	member := feedVideoMember(event.AuthorID, event.VideoID)
	for _, followerID := range followerIDs {
		if followerID == 0 {
			continue
		}
		inboxKey := c.cache.FollowerInboxKey(followerID)
		pipe.ZAdd(ctx, inboxKey, redis.Z{
			Score:  float64(event.CreateTime),
			Member: member,
		})

		// if rand.Float64() < 0.01 {
		// 	pipe.ZRemRangeByRank(ctx, outboxKey, 0, -int64(maxOutboxItems)-1)
		// }

		pipe.ZRemRangeByRank(ctx, inboxKey, 0, -int64(maxInboxItems)-1)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// FollowAuthor backfills ordinary authors. Big creator outboxes are shared and
// selected from the current MySQL follow relationship when the feed is read.
func (c *FeedCache) FollowAuthor(ctx context.Context, followerID, authorID uint, bigCreator bool, videos []mq.FanoutEvent) error {
	if c == nil || c.cache == nil {
		return errors.New("cache is nil")
	}
	if followerID == 0 || authorID == 0 {
		return errors.New("IDs must be positive")
	}

	if !bigCreator {
		return c.BackfillInbox(ctx, followerID, authorID, videos)
	}
	return nil
}

// RemoveFollow removes the author's known videos from the follower's inbox.
func (c *FeedCache) RemoveFollow(ctx context.Context, followerID, authorID uint, videos []mq.FanoutEvent) error {
	if c == nil || c.cache == nil {
		return errors.New("cache is nil")
	}
	if followerID == 0 || authorID == 0 {
		return errors.New("IDs must be positive")
	}
	pipe := c.cache.Pipeline()
	if pipe == nil {
		return errors.New("redis pipeline is nil")
	}
	inboxKey := c.cache.FollowerInboxKey(followerID)
	for _, event := range videos {
		if event.AuthorID == authorID && event.VideoID != 0 {
			// Remove the new author:video member format and legacy bare video IDs.
			pipe.ZRem(ctx, inboxKey, feedVideoMember(authorID, event.VideoID), fmt.Sprintf("%d", event.VideoID))
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

// BackfillInbox adds existing author videos to one follower's inbox.
func (c *FeedCache) BackfillInbox(ctx context.Context, followerID, authorID uint, videos []mq.FanoutEvent) error {
	if c == nil || c.cache == nil {
		return errors.New("cache is nil")
	}
	if followerID == 0 || authorID == 0 {
		return errors.New("IDs must be positive")
	}
	pipe := c.cache.Pipeline()
	if pipe == nil {
		return errors.New("redis pipeline is nil")
	}
	inboxKey := c.cache.FollowerInboxKey(followerID)
	for _, event := range videos {
		if event.AuthorID == authorID && event.VideoID != 0 {
			pipe.ZAdd(ctx, inboxKey, redis.Z{
				Score:  float64(event.CreateTime),
				Member: feedVideoMember(authorID, event.VideoID),
			})
		}
	}
	pipe.ZRemRangeByRank(ctx, inboxKey, 0, -int64(maxInboxItems)-1)
	_, err := pipe.Exec(ctx)
	return err
}

func feedVideoMember(authorID, videoID uint) string {
	return fmt.Sprintf("%d:%d", authorID, videoID)
}
