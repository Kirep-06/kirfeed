package rediscache

import (
	"context"
	"errors"
	"fmt"
	"kirfeed/internal/config"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	redisv9 "github.com/redis/go-redis/v9"
)

type Client struct {
	rdb       *redisv9.Client
	keyPrefix string
}

const defaultKeyPrefix = "v1:"

func NewClient(rdb *redisv9.Client, keyPrefix string) *Client {
	return &Client{
		rdb:       rdb,
		keyPrefix: keyPrefix,
	}
}

func NewFromEnv(cfg *config.RedisConfig) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Host + ":" + strconv.Itoa(cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	return &Client{rdb: rdb, keyPrefix: defaultKeyPrefix}, nil
}

func (c *Client) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return c.rdb.Ping(ctx).Err()
}

func (c *Client) Key(format string, args ...any) string {
	prefix := ""
	if c != nil {
		prefix = c.keyPrefix
	}
	return prefix + fmt.Sprintf(format, args...)
}

func (c *Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.Get(ctx, key).Bytes()
}

func (c *Client) SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return c.rdb.Set(ctx, key, value, ttl).Err()
}

func (c *Client) Del(ctx context.Context, key string) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return c.rdb.Del(ctx, key).Err()
}
