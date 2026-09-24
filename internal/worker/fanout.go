package worker

import (
	"context"
	"encoding/json"
	"errors"
	"kirfeed/internal/infra/mq"
	"kirfeed/internal/video"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type FanoutWorker struct {
	ch     *amqp.Channel
	videos *video.VideoRepository
	queue  string
}

func NewFanoutWorker(ch *amqp.Channel, videos *video.VideoRepository, queue string) *FanoutWorker {
	return &FanoutWorker{
		ch:     ch,
		videos: videos,
		queue:  queue,
	}
}

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
				_ = d.Ack(false)
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

	_ = ctx
	return nil
}
