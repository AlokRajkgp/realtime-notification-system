// Command api runs the producer REST API: it accepts events over HTTP,
// publishes them to Kafka, serves delivery-status queries, and hosts the
// in-app notification WebSocket. It does not talk to the Kafka-consuming
// side of channel adapters (email/push) — that's the worker's job.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"realtime-notification-system/internal/api"
	"realtime-notification-system/internal/config"
	"realtime-notification-system/internal/db"
	"realtime-notification-system/internal/kafkaclient"
	"realtime-notification-system/internal/ws"
)

// shutdownGrace is how long in-flight HTTP requests get to finish once a
// shutdown signal arrives before the server gives up on them and exits
// anyway. Deliberately does not cover open WebSocket connections — see the
// comment above srv.Shutdown below.
const shutdownGrace = 10 * time.Second

func main() {
	cfg := config.Load()

	producer := kafkaclient.NewProducer(cfg.KafkaBrokers, cfg.KafkaEventsTopic)
	defer producer.Close()

	conn, err := db.Connect(cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("api: connect postgres: %v", err)
	}
	defer conn.Close()
	store := db.NewStore(conn)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
	defer rdb.Close()
	hub := ws.NewHub(rdb)

	server := api.NewServer(producer, store)
	router := api.NewRouter(server, hub)

	httpServer := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: router,
	}

	// Run the server in a goroutine so main() is free to block on the
	// shutdown signal below instead of inside ListenAndServe.
	go func() {
		log.Printf("api: listening on :%s (kafka brokers=%v topic=%s)", cfg.HTTPPort, cfg.KafkaBrokers, cfg.KafkaEventsTopic)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("api: server error: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	<-ctx.Done()
	stop() // restore default signal behavior: a second Ctrl+C/SIGTERM force-kills instead of queuing
	log.Println("api: shutdown signal received, draining in-flight requests...")

	// http.Server.Shutdown stops accepting new connections immediately and
	// waits (up to shutdownGrace) for in-flight requests to return on their
	// own -- it does NOT forcibly cancel their request context, so a
	// request mid-write to Kafka/Postgres is allowed to finish rather than
	// being aborted mid-write.
	//
	// One deliberate gap: Shutdown does not track or wait for hijacked
	// connections, and a WebSocket upgrade (internal/ws.Hub.ServeWS) is
	// exactly that -- it takes the TCP connection out of net/http's normal
	// request lifecycle via http.Hijacker. So an open WebSocket is not
	// notified or drained here; it simply stays open until the process
	// actually exits, at which point the OS closes the socket. For this
	// project's size that's an accepted, documented limitation rather than
	// something worth building extra machinery for.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("api: graceful shutdown did not finish in time, forcing close: %v", err)
	}

	log.Println("api: closing producer/db/redis")
}
