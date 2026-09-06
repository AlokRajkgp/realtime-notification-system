// Command worker is the consumer group: it reads events off Kafka and
// drives each one through the delivery state machine (internal/delivery) —
// claim, check preferences, send via the right channel adapter, and record
// the outcome, retrying transient failures with backoff and dead-lettering
// permanent ones or exhausted retries.
//
// Two independent loops run in this process:
//   - the Kafka fetch loop: turns each new message into a claim (first
//     sight of an event, attempt 1)
//   - the reclaim-scan loop: a single extra goroutine (NOT a worker pool —
//     it does one job, sequentially) that periodically finds retryable
//     deliveries whose backoff has elapsed, or pending deliveries whose
//     claim has gone stale (their worker likely crashed), and re-drives
//     them through the exact same processing logic. This is what actually
//     fixes the "crash after claim, stuck pending forever" failure mode:
//     once a claim goes stale, this loop reclaims and retries it -- Kafka
//     redelivery is no longer what drives retries past the first attempt.
//
// Run 2-3 instances (same KAFKA_GROUP_ID) to see Kafka spread partitions
// across them (the topic needs more than 1 partition for that to do
// anything — see `make kafka-topic`).
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
	"realtime-notification-system/internal/delivery"
	"realtime-notification-system/internal/kafkaclient"
	"realtime-notification-system/internal/models"
	"realtime-notification-system/internal/preferences"
)

// processTimeout bounds how long a single already-fetched message is given
// to finish (claim + preference check + adapter sends + commit) once a
// shutdown signal has arrived. Deliberately NOT the same context used to
// wait for the *next* message — see the comment on rootCtx below.
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

	dlqProducer := kafkaclient.NewProducer(cfg.KafkaBrokers, cfg.KafkaDLQTopic)
	defer dlqProducer.Close()

	// Every event is offered to every adapter in this list; whether it's
	// actually sent depends on that user's preferences.
	proc := delivery.NewProcessor(store, checker, dlqProducer, cfg.RetryPolicy, []adapters.Adapter{
		adapters.NewInApp(rdb),
	})

	consumer := kafkaclient.NewConsumer(cfg.KafkaBrokers, cfg.KafkaEventsTopic, cfg.KafkaGroupID)
	defer consumer.Close()

	pid := os.Getpid()

	// rootCtx is cancelled the instant SIGINT/SIGTERM arrives. It is used
	// ONLY to unblock the "wait for the next message" call and the reclaim
	// loop's ticker wait — it is deliberately never passed into an
	// in-flight process() call, because pgx and go-redis both honor
	// context cancellation and would abort a half-done DB write or Redis
	// publish mid-flight. That would turn a clean shutdown into the exact
	// "crashed mid-processing" failure case this phase exists to handle —
	// self-inflicted, avoidably. Work already in hand gets its own
	// bounded-but-separate context (processTimeout) instead.
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The reclaim-scan loop: one dedicated goroutine, one job, running
	// sequentially -- not a worker pool. It's what re-drives retries and
	// recovers stale claims; see the package doc above.
	reclaimDone := make(chan struct{})
	go func() {
		defer close(reclaimDone)
		ticker := time.NewTicker(cfg.ReclaimInterval)
		defer ticker.Stop()
		for {
			select {
			case <-rootCtx.Done():
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), processTimeout)
				n, err := proc.ReclaimSweep(ctx)
				cancel()
				if err != nil {
					log.Printf("worker: pid=%d reclaim sweep error: %v", pid, err)
				} else if n > 0 {
					log.Printf("worker: pid=%d reclaim sweep reclaimed %d row(s)", pid, n)
				}
			}
		}
	}()

	log.Printf("worker: pid=%d consuming topic=%s group=%s brokers=%v reclaim_interval=%s stale_claim_timeout=%s max_attempts=%d",
		pid, cfg.KafkaEventsTopic, cfg.KafkaGroupID, cfg.KafkaBrokers, cfg.ReclaimInterval, cfg.RetryPolicy.StaleClaimTimeout, cfg.RetryPolicy.MaxAttempts)

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
			// partition forever. Commit past it now; there's no retryable
			// delivery_status row to route to the DLQ since we never even
			// got as far as claiming one.
			log.Printf("worker: skipping unparseable message at partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)
			_ = consumer.Commit(procCtx, msg)
		} else {
			log.Printf("worker: pid=%d fetched partition=%d offset=%d event=%s user=%s", pid, msg.Partition, msg.Offset, event.EventID, event.UserID)

			if proc.HandleNew(procCtx, event) {
				if err := consumer.Commit(procCtx, msg); err != nil {
					log.Printf("worker: commit failed partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)
				}
			} else {
				log.Printf("worker: not committing partition=%d offset=%d: claim failed (Postgres unreachable?), will be redelivered", msg.Partition, msg.Offset)
			}
		}

		cancel()

		if rootCtx.Err() != nil {
			log.Printf("worker: pid=%d shutdown signal received, exiting after finishing in-flight message", pid)
			break runLoop
		}
	}

	<-reclaimDone
	log.Printf("worker: pid=%d closed cleanly", pid)
}
