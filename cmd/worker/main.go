// Command worker is the consumer group: it reads events off Kafka and, for
// each channel it knows about, idempotently claims and records a delivery
// attempt in Postgres. Run 2-3 instances (same KAFKA_GROUP_ID) to see Kafka
// spread partitions across them.
//
// What this step deliberately does NOT do yet (later roadmap steps):
//   - real channel adapters (in-app WS/SSE, email, push) — delivery is a
//     log line for now, standing in for "sent"
//   - per-user preferences (opt-in/out, DND) — every event goes to every
//     channel in the static `channels` list below
//   - retry-with-backoff / dead-letter queue — a failed claim or mark is
//     logged and the offset is still committed, so today a transient
//     Postgres error means that (event_id, channel) is simply never
//     retried. That gap is exactly what the DLQ + admin-replay step fixes.
package main

import (
	"context"
	"encoding/json"
	"log"

	"realtime-notification-system/internal/config"
	"realtime-notification-system/internal/db"
	"realtime-notification-system/internal/kafkaclient"
	"realtime-notification-system/internal/models"
)

// Channels this worker currently delivers every event to. Once user
// preferences exist, this becomes per-user/per-event instead of static.
var channels = []string{"in-app"}

func main() {
	cfg := config.Load()

	conn, err := db.Connect(cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("worker: connect postgres: %v", err)
	}
	defer conn.Close()
	store := db.NewStore(conn)

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

		for _, channel := range channels {
			claimed, err := store.ClaimDelivery(ctx, event.EventID, event.UserID, channel)
			if err != nil {
				log.Printf("worker: claim failed event=%s channel=%s: %v", event.EventID, channel, err)
				continue
			}
			if !claimed {
				log.Printf("worker: duplicate delivery skipped event=%s channel=%s", event.EventID, channel)
				continue
			}

			// Stand-in "adapter" — real email/push/in-app delivery is a later step.
			log.Printf("worker: delivering event=%s user=%s type=%s via channel=%s", event.EventID, event.UserID, event.Type, channel)

			if err := store.MarkStatus(ctx, event.EventID, channel, "sent", nil); err != nil {
				log.Printf("worker: mark status failed event=%s channel=%s: %v", event.EventID, channel, err)
			}
		}

		if err := consumer.Commit(ctx, msg); err != nil {
			log.Printf("worker: commit failed offset=%d: %v", msg.Offset, err)
		}
	}
}
