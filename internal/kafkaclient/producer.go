// Package kafkaclient wraps segmentio/kafka-go so the rest of the codebase
// never imports kafka-go directly.
package kafkaclient

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"

	"realtime-notification-system/internal/models"
)

// Producer publishes events to a single Kafka topic, keyed by user ID so
// that all events for one user land on the same partition (and therefore
// keep their order for any one consumer).
type Producer struct {
	writer *kafka.Writer
}

// NewProducer builds a Producer. brokers is a list of "host:port" addresses.
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // hashes Message.Key -> deterministic partition per user
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
	}
}

// Publish JSON-encodes the event and writes it to Kafka with the user ID as
// the partition key.
func (p *Producer) Publish(ctx context.Context, event models.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.UserID),
		Value: body,
	})
}

// Close flushes and closes the underlying Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
