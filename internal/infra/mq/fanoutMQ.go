package mq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type FanoutMQ struct {
	ch *amqp.Channel
}

const (
	fanoutExchange   = "video.fanout.events"
	fanoutQueue      = "video.fanout.queue"
	fanoutBindingKey = "video.fanout.*"
	
	fanoutPublishRK  = "video.fanout.publish"
)

type FanoutEvent struct {
	EventID    string    `json:"event_id"`
	VideoID    uint      `json:"video_id"`
	AuthorID   uint      `json:"author_id"`
	CreateTime int64     `json:"create_time"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewFanoutMQ(base *RabbitMQ) (*FanoutMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	ch, err := base.NewChannel()
	if err != nil {
		return nil, err
	}
	if err := DeclareTopic(ch, fanoutExchange, fanoutQueue, fanoutBindingKey); err != nil {
		_ = ch.Close()
		return nil, err
	}
	return &FanoutMQ{ch: ch}, nil
}

// PublishVideo 将新视频投递给关注流分发 Worker。
func (f *FanoutMQ) PublishVideo(ctx context.Context, videoID, authorID uint, createTime time.Time) error {
	if f == nil || f.ch == nil {
		return errors.New("fanout mq is not initialized")
	}
	if videoID == 0 || authorID == 0 {
		return errors.New("videoID and authorID are required")
	}
	if createTime.IsZero() {
		return errors.New("createTime is required")
	}
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	event := FanoutEvent{
		EventID:    id,
		VideoID:    videoID,
		AuthorID:   authorID,
		CreateTime: createTime.UnixMilli(),
		OccurredAt: time.Now().UTC(),
	}
	return PublishJSON(ctx, f.ch, fanoutExchange, fanoutPublishRK, event)
}

func (f *FanoutMQ) Close() error {
	if f == nil || f.ch == nil {
		return nil
	}
	return f.ch.Close()
}
