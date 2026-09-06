// Package config loads runtime configuration from environment variables
// (with a .env file for local dev). Kept deliberately simple: a plain
// struct populated with os.Getenv + defaults, no config-framework dependency.
package config

import (
	"os"

	"github.com/joho/godotenv"
)

// Config holds every setting the api/worker binaries need.
type Config struct {
	// HTTP
	HTTPPort string

	// Kafka
	KafkaBrokers   []string // comma-separated in env, split here
	KafkaEventsTopic string
	KafkaGroupID   string

	// Postgres
	PostgresDSN string

	// Redis
	RedisAddr     string
	RedisPassword string
}

// Load reads a .env file if present (local dev convenience — it's a no-op
// if the file doesn't exist, e.g. in a container where real env vars are
// injected) and then builds a Config from the environment.
func Load() Config {
	_ = godotenv.Load()

	return Config{
		HTTPPort: getEnv("HTTP_PORT", "8080"),

		KafkaBrokers:     splitCSV(getEnv("KAFKA_BROKERS", "localhost:19092")),
		KafkaEventsTopic: getEnv("KAFKA_EVENTS_TOPIC", "notifications.events"),
		KafkaGroupID:     getEnv("KAFKA_GROUP_ID", "notification-workers"),

		PostgresDSN: getEnv("POSTGRES_DSN", "postgres://notify:notify@localhost:5432/notify?sslmode=disable"),

		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitCSV(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
