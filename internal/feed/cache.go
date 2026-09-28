package feed

import (
	"context"
	"errors"
	"fmt"
	rediscache "kirfeed/internal/infra/cache"
	"kirfeed/internal/infra/mq"

	"github.com/redis/go-redis/v9"
)

type FeedCache struct {
	cache rediscache.Client
}

func (c *FeedCache) addAuthorOutbox(ctx context.Context, event mq.FanoutEvent) error {
	if w.cache == nil {
		return errors.New("cache is nil")
	}

	outboxkey := w.cache.Key("%s", rediscache.AuthorOutboxKey(event.AuthorID))
	if err := w.cache.ZAdd(ctx, outboxkey, redis.Z{
		Score:  float64(event.CreateTime),
		Member: fmt.Sprintf("%d", event.VideoID),
	}); err != nil {
		return err
	}
	return w.cache.ZRemRangeByRank(ctx, outboxkey, 0, -501)
}

func (c *FeedCache) addFollowerInbox(ctx context.Context, event mq.FanoutEvent) error {
	if w.cache == nil || w.social == nil {
		return errors.New("cache or social repository is nil")
	}

	const batchSize = 100
	var cursor uint

	for {
		followerIDs, nextCursor, hasMore, err :=
			w.social.ListFollowerIDs(ctx, event.AuthorID, batchSize, cursor)
		if err != nil {
			return err
		}
		if len(followerIDs) == 0 {
			return nil
		}
		pipe := w.cache.Pipeline()
		if pipe == nil {
			return errors.New("redis pipeline is nil")
		}
		for _, fid := range followerIDs {
			inboxkey := w.cache.Key("%s", rediscache.FollowerInboxKey(fid))
			pipe.ZAdd(ctx, inboxkey, redis.Z{
				Score:  float64(event.CreateTime),
				Member: fmt.Sprintf("%d", event.VideoID),
			})
			pipe.ZRemRangeByRank(ctx, inboxkey, 0, -501)
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
