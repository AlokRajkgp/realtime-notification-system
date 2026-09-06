// Command api runs the producer REST API: it accepts events over HTTP,
// publishes them to Kafka, serves delivery-status queries, and hosts the
// in-app notification WebSocket. It does not talk to the Kafka-consuming
// side of channel adapters (email/push) — that's the worker's job.
package main

import (
	"log"

	"github.com/redis/go-redis/v9"

	"realtime-notification-system/internal/api"
	"realtime-notification-system/internal/config"
	"realtime-notification-system/internal/db"
	"realtime-notification-system/internal/kafkaclient"
	"realtime-notification-system/internal/ws"
)

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

	log.Printf("api: listening on :%s (kafka brokers=%v topic=%s)", cfg.HTTPPort, cfg.KafkaBrokers, cfg.KafkaEventsTopic)
	if err := router.Run(":" + cfg.HTTPPort); err != nil {
		log.Fatalf("api: server error: %v", err)
	}
}
