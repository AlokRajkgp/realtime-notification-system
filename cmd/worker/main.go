// Command worker is the consumer group: it reads events off Kafka and, for
// each registered channel adapter, idempotently claims and records a
// delivery attempt in Postgres. Run 2-3 instances (same KAFKA_GROUP_ID) to
// see Kafka spread partitions across them.
//
// What this step deliberately does NOT do yet (later roadmap steps):
//   - email/push channel adapters — only "in-app" (Redis Pub/Sub, see
//     internal/adapters) is registered so far
//   - per-user preferences (opt-in/out, DND) — every event goes to every
//     registered adapter
//   - retry-with-backoff / dead-letter queue — a failed claim or send is
//     logged and the offset is still committed, so today a transient
//     failure means that (event_id, channel) is simply never retried.
//     That gap is exactly what the DLQ + admin-replay step fixes.
package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"

	"realtime-notification-system/internal/adapters"
	"realtime-notification-system/internal/config"
	"realtime-notification-system/internal/db"
	"realtime-notification-system/internal/kafkaclient"
	"realtime-notification-system/internal/models"
)

func main() {
	cfg := config.Load()

	conn, err := db.Connect(cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("worker: connect postgres: %v", err)
	}
	defer conn.Close()
	store := db.NewStore(conn)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
	defer rdb.Close()

	// Every event goes to every adapter in this list. Once user preferences
	// exist, this becomes a per-user/per-event selection instead of static.
	channelAdapters := []adapters.Adapter{
		adapters.NewInApp(rdb),
	}

	consumer := kafkaclient.NewConsumer(cfg.KafkaBrokers, cfg.KafkaEventsTopic, cfg.KafkaGroupID)
	defer consumer.Close()

	ctx := context.Background()
	log.Printf("worker: consuming topic=%s group=%s brokers=%v", cfg.KafkaEventsTopic, cfg.KafkaGroupID, cfg.KafkaBrokers)

	for {
		msg, err := consumer.FetchMessage(ctx)
		if err != nil {
			log.Printf("worker: fetch error: %v", err)
			continue
		}

		var event models.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			// A message we can never parse would otherwise block this
			// partition forever. Commit past it now; routing it to a DLQ
			// instead of dropping it is the retry/DLQ step's job.
			log.Printf("worker: skipping unparseable message at offset %d: %v", msg.Offset, err)
			_ = consumer.Commit(ctx, msg)
			continue
		}

		for _, adapter := range channelAdapters {
			deliver(ctx, store, adapter, event)
		}

		if err := consumer.Commit(ctx, msg); err != nil {
			log.Printf("worker: commit failed offset=%d: %v", msg.Offset, err)
		}
	}
}

// deliver claims (event, adapter) idempotently, sends via the adapter if
// this call won the claim, and records the outcome.
func deliver(ctx context.Context, store *db.Store, adapter adapters.Adapter, event models.Event) {
	channel := adapter.Name()

	claimed, err := store.ClaimDelivery(ctx, event.EventID, event.UserID, channel)
	if err != nil {
		log.Printf("worker: claim failed event=%s channel=%s: %v", event.EventID, channel, err)
		return
	}
	if !claimed {
		log.Printf("worker: duplicate delivery skipped event=%s channel=%s", event.EventID, channel)
		return
	}

	sendErr := adapter.Send(ctx, event)
	status := "sent"
	if sendErr != nil {
		status = "failed"
		log.Printf("worker: delivery failed event=%s channel=%s: %v", event.EventID, channel, sendErr)
	} else {
		log.Printf("worker: delivered event=%s user=%s type=%s via channel=%s", event.EventID, event.UserID, event.Type, channel)
	}

	if err := store.MarkStatus(ctx, event.EventID, channel, status, sendErr); err != nil {
		log.Printf("worker: mark status failed event=%s channel=%s: %v", event.EventID, channel, err)
	}
}
