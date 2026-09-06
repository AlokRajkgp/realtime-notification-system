// Command worker is the consumer group: it reads events off Kafka and, for
// each registered channel adapter, idempotently claims and records a
// delivery attempt in Postgres — honoring per-user channel opt-in/out and
// DND windows along the way. Run 2-3 instances (same KAFKA_GROUP_ID) to see
// Kafka spread partitions across them (the topic needs more than 1
// partition for that to do anything — see `make kafka-topic`).
//
// What this step deliberately does NOT do yet (later roadmap steps):
//   - email/push channel adapters — only "in-app" (Redis Pub/Sub, see
//     internal/adapters) is registered so far
//   - retry-with-backoff / dead-letter queue — a failed claim or send is
//     logged and the offset is still committed, so today a transient
//     failure means that (event_id, channel) is simply never retried.
//     That gap is exactly what the DLQ + admin-replay step fixes.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"realtime-notification-system/internal/adapters"
	"realtime-notification-system/internal/config"
	"realtime-notification-system/internal/db"
	"realtime-notification-system/internal/kafkaclient"
	"realtime-notification-system/internal/models"
	"realtime-notification-system/internal/preferences"
)

// processTimeout bounds how long a single already-fetched message is given
// to finish (claim + preference check + adapter sends + commit) once a
// shutdown signal has arrived. It's intentionally NOT the same context used
// to wait for the *next* message — see the comment on rootCtx below.
const processTimeout = 15 * time.Second

func main() {
	cfg := config.Load()

	conn, err := db.Connect(cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("worker: connect postgres: %v", err)
	}
	defer conn.Close()
	store := db.NewStore(conn)
	checker := preferences.NewChecker(store)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
	defer rdb.Close()

	// Every event is offered to every adapter in this list; whether it's
	// actually sent depends on that user's preferences (see deliver below).
	channelAdapters := []adapters.Adapter{
		adapters.NewInApp(rdb),
	}

	consumer := kafkaclient.NewConsumer(cfg.KafkaBrokers, cfg.KafkaEventsTopic, cfg.KafkaGroupID)
	defer consumer.Close()

	pid := os.Getpid()

	// rootCtx is cancelled the instant SIGINT/SIGTERM arrives. It is used
	// ONLY to unblock the "wait for the next message" call below — it is
	// deliberately never passed into deliver() for a message already in
	// hand, because pgx and go-redis both honor context cancellation and
	// would abort a half-done DB write or Redis publish mid-flight. That
	// would turn a clean shutdown into the exact "crashed mid-processing"
	// failure case the audit flagged — self-inflicted, avoidably. A message
	// already fetched gets its own bounded-but-separate context
	// (processTimeout) instead, so it's allowed to finish.
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("worker: pid=%d consuming topic=%s group=%s brokers=%v", pid, cfg.KafkaEventsTopic, cfg.KafkaGroupID, cfg.KafkaBrokers)

runLoop:
	for {
		msg, err := consumer.FetchMessage(rootCtx)
		if err != nil {
			if rootCtx.Err() != nil {
				log.Printf("worker: pid=%d shutdown signal received, no message in flight", pid)
				break runLoop
			}
			log.Printf("worker: fetch error: %v", err)
			continue
		}

		procCtx, cancel := context.WithTimeout(context.Background(), processTimeout)

		var event models.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			// A message we can never parse would otherwise block this
			// partition forever. Commit past it now; routing it to a DLQ
			// instead of dropping it is the retry/DLQ step's job.
			log.Printf("worker: skipping unparseable message at partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)
			_ = consumer.Commit(procCtx, msg)
		} else {
			log.Printf("worker: pid=%d fetched partition=%d offset=%d event=%s user=%s", pid, msg.Partition, msg.Offset, event.EventID, event.UserID)

			for _, adapter := range channelAdapters {
				deliver(procCtx, store, checker, adapter, event)
			}

			if err := consumer.Commit(procCtx, msg); err != nil {
				log.Printf("worker: commit failed partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)
			}
		}

		cancel()

		if rootCtx.Err() != nil {
			log.Printf("worker: pid=%d shutdown signal received, exiting after finishing in-flight message", pid)
			break runLoop
		}
	}

	log.Printf("worker: pid=%d closed cleanly", pid)
}

// deliver claims (event, adapter) idempotently, checks the user's
// preferences, and only then sends via the adapter — recording whichever
// outcome actually happened.
func deliver(ctx context.Context, store *db.Store, checker *preferences.Checker, adapter adapters.Adapter, event models.Event) {
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

	allowed, err := checker.Allowed(ctx, event.UserID, channel)
	if err != nil {
		// Fail closed: record it as failed (visible in delivery_status)
		// rather than guessing whether the user actually wanted this.
		log.Printf("worker: preference check failed event=%s channel=%s: %v", event.EventID, channel, err)
		_ = store.MarkStatus(ctx, event.EventID, channel, "failed", err)
		return
	}
	if !allowed {
		log.Printf("worker: skipped by preferences event=%s channel=%s", event.EventID, channel)
		_ = store.MarkStatus(ctx, event.EventID, channel, "skipped", nil)
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
