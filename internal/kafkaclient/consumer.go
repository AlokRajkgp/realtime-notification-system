package kafkaclient

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Consumer wraps a kafka-go Reader configured for consumer-group semantics:
// setting GroupID makes the brokers assign this reader a subset of the
// topic's partitions (shared with every other reader using the same
// GroupID), and track this group's committed offsets. Running 2-3 worker
// instances with the same GroupID is what gives us the "consumer group"
// from the project scope — Kafka rebalances partitions across whichever
// instances are alive.
type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
	}
}

// FetchMessage blocks until the next message is available. We use
// Fetch+Commit (manual) rather than ReadMessage (which auto-commits) so we
// control exactly when an offset is considered "done" — see Commit.
func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

// Commit advances this consumer group's committed offset past msg. Until
// this is called, a restart or rebalance of this consumer group will
// re-deliver msg to whichever instance picks up its partition — which is
// safe because delivery is claimed idempotently in Postgres (see internal/db).
func (c *Consumer) Commit(ctx context.Context, msg kafka.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
