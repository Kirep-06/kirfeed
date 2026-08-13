package cache

import (
	"context"
	"errors"
	"kirfeed/internal/config"
	"strconv"

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
