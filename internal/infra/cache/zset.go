package rediscache

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func (c *Client) ZincrBy(ctx context.Context, key string, member string, score float64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZIncrBy(ctx, key, score, member).Err()
}

func (c *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZAdd(ctx, key, members...).Err()
}

func (c *Client) ZRem(ctx context.Context, key string, members ...interface{}) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZRem(ctx, key, members...).Err()
}

func (c *Client) ZRemRangeByRank(ctx context.Context, key string, start int64, stop int64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZRemRangeByRank(ctx, key, start, stop).Err()
}

func (c *Client) ZRangeWithScores(ctx context.Context, key string, start int64, stop int64) ([]redis.Z, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.ZRangeWithScores(ctx, key, start, stop).Result()
}

func (c *Client) HmGet(ctx context.Context, key string, fields ...string) ([]interface{}, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.HMGet(ctx, key, fields...).Result()
}

func (c *Client) HSet(ctx context.Context, key string, values ...interface{}) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return c.rdb.HSet(ctx, key, values...).Err()
}

func (c *Client) HDel(ctx context.Context, key string, fields ...string) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return c.rdb.HDel(ctx, key, fields...).Err()
}

var invalidateIndexScript = redis.NewScript(`
redis.call('INCR', KEYS[1])
redis.call('DEL', KEYS[2])
return 1
`)

var rebuildIndexScript = redis.NewScript(`
local generation = redis.call('GET', KEYS[1]) or '0'
if generation ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[2])
redis.call('ZADD', KEYS[2], -1, '__ready__:' .. ARGV[3])
for i = 4, #ARGV, 2 do
  redis.call('ZADD', KEYS[2], ARGV[i], ARGV[i + 1])
end
redis.call('PEXPIRE', KEYS[2], ARGV[2])
return 1
`)

var rebuildCountScript = redis.NewScript(`
local generation = redis.call('GET', KEYS[1]) or '0'
if generation ~= ARGV[1] then return 0 end
redis.call('PSETEX', KEYS[2], ARGV[2], ARGV[3])
return 1
`)

func (c *Client) ReadCount(ctx context.Context, key string) (int64, bool, error) {
	if c == nil || c.rdb == nil {
		return 0, false, errors.New("redis client not initialized")
	}
	value, err := c.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	count, err := strconv.ParseInt(value, 10, 64)
	if err != nil || count < 0 {
		return 0, false, errors.New("invalid cached count")
	}
	return count, true, nil
}

func (c *Client) StoreCountIfGeneration(ctx context.Context, generationKey, key, generation string, count int64, ttl time.Duration) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, errors.New("redis client not initialized")
	}
	result, err := rebuildCountScript.Run(ctx, c.rdb, []string{generationKey, key}, generation, strconv.FormatInt(ttl.Milliseconds(), 10), strconv.FormatInt(count, 10)).Int()
	return result == 1, err
}

func (c *Client) IndexGeneration(ctx context.Context, generationKey string) (string, error) {
	if c == nil || c.rdb == nil {
		return "", errors.New("redis client not initialized")
	}
	value, err := c.rdb.Get(ctx, generationKey).Result()
	if err == redis.Nil {
		return "0", nil
	}
	return value, err
}

func (c *Client) InvalidateZSetIndex(ctx context.Context, generationKey, key string) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return invalidateIndexScript.Run(ctx, c.rdb, []string{generationKey, key}).Err()
}

// InvalidateSocialRelationship invalidates both sides of one follow change atomically.
func (c *Client) InvalidateSocialRelationship(ctx context.Context, followerID, followeeID uint) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	keys := []string{
		c.FollowingRelationsGenerationKey(followerID), c.FollowingRelationsKey(followerID),
		c.RecentFollowersGenerationKey(followeeID), c.RecentFollowersKey(followeeID),
		c.FollowingCountGenerationKey(followerID), c.FollowingCountKey(followerID),
		c.FollowersCountGenerationKey(followeeID), c.FollowersCountKey(followeeID),
	}
	return invalidateSocialRelationshipScript.Run(ctx, c.rdb, keys).Err()
}

var invalidateSocialRelationshipScript = redis.NewScript(`
for i = 1, #KEYS, 2 do
  redis.call('INCR', KEYS[i])
  redis.call('DEL', KEYS[i + 1])
end
return 1
`)

func (c *Client) StoreZSetIfGeneration(ctx context.Context, generationKey, key, generation string, members []redis.Z, count int, ttl time.Duration) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, errors.New("redis client not initialized")
	}
	args := make([]interface{}, 0, 3+2*len(members))
	args = append(args, generation, strconv.FormatInt(ttl.Milliseconds(), 10), strconv.Itoa(count))
	for _, member := range members {
		args = append(args, strconv.FormatFloat(member.Score, 'f', 0, 64), member.Member)
	}
	result, err := rebuildIndexScript.Run(ctx, c.rdb, []string{generationKey, key}, args...).Int()
	return result == 1, err
}

func (c *Client) ReadZSetIndex(ctx context.Context, key string, maxMembers int) ([]redis.Z, int, bool, error) {
	members, err := c.ZRangeWithScores(ctx, key, 0, -1)
	if err != nil || len(members) == 0 {
		return nil, 0, false, err
	}
	var expected int = -1
	entries := make([]redis.Z, 0, len(members)-1)
	for _, member := range members {
		value, ok := member.Member.(string)
		if !ok {
			return nil, 0, false, nil
		}
		if len(value) >= 10 && value[:10] == "__ready__:" {
			if expected >= 0 {
				return nil, 0, false, nil
			}
			count, parseErr := strconv.Atoi(value[10:])
			if parseErr != nil || count < 0 {
				return nil, 0, false, nil
			}
			expected = count
			continue
		}
		entries = append(entries, member)
	}
	want := expected
	if maxMembers > 0 && want > maxMembers {
		want = maxMembers
	}
	if expected < 0 || want != len(entries) {
		return nil, 0, false, nil
	}
	return entries, expected, true, nil
}

func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Expire(ctx, key, ttl).Err()
}

func (c *Client) ZUnionStore(ctx context.Context, dst string, keys []string, aggregate string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZUnionStore(ctx, dst, &redis.ZStore{
		Keys:      keys,
		Aggregate: aggregate,
	}).Err()
}

func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, nil
	}
	n, err := c.rdb.Exists(ctx, key).Result()
	return n > 0, err
}

func (c *Client) ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key: key, Start: start, Stop: stop, Rev: true,
	}).Result()
}

func (c *Client) ZRevRangeByScore(ctx context.Context, key string, max, min string, offset, count int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     key,
		Start:   max,
		Stop:    min,
		ByScore: true,
		Rev:     true,
		Offset:  offset,
		Count:   count,
	}).Result()
}
