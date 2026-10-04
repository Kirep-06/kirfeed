package video

import (
	"context"
	rediscache "kirfeed/internal/infra/cache"
	"strconv"
	"time"
)

func UpdatePopularityCache(ctx context.Context, cache *rediscache.Client, id uint, change int64) {
	if cache == nil || id == 0 || change == 0 {
		return
	}

	_ = cache.Del(context.Background(), cache.VideoDetailKey(id))
	_ = cache.Del(context.Background(), cache.VideoEntityKey(id))

	now := time.Now().UTC().Truncate(time.Minute)
	windowKey := cache.HotVideoMinuteKey(now)
	member := strconv.FormatUint(uint64(id), 10)

	opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	_ = cache.ZincrBy(opCtx, windowKey, member, float64(change))
	_ = cache.Expire(opCtx, windowKey, 2*time.Hour)
}
