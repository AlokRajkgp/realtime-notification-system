// Command api runs the producer REST API: it accepts events over HTTP and
// publishes them to Kafka. It does not talk to channel adapters (email/push/
// in-app) directly — that's the worker's job, built in a later step.
package main

import (
	"log"

	"realtime-notification-system/internal/api"
	"realtime-notification-system/internal/config"
	"realtime-notification-system/internal/kafkaclient"
)

func main() {
	cfg := config.Load()

	producer := kafkaclient.NewProducer(cfg.KafkaBrokers, cfg.KafkaEventsTopic)
	defer producer.Close()

	server := api.NewServer(producer)
	router := api.NewRouter(server)

	log.Printf("api: listening on :%s (kafka brokers=%v topic=%s)", cfg.HTTPPort, cfg.KafkaBrokers, cfg.KafkaEventsTopic)
	if err := router.Run(":" + cfg.HTTPPort); err != nil {
		log.Fatalf("api: server error: %v", err)
	}
}
