// Package config loads runtime configuration from environment variables
// (with a .env file for local dev). Kept deliberately simple: a plain
// struct populated with os.Getenv + defaults, no config-framework dependency.
package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"

	"realtime-notification-system/internal/retry"
)

// Config holds every setting the api/worker binaries need.
type Config struct {
	// HTTP
	HTTPPort string

	// Kafka
	KafkaBrokers     []string // comma-separated in env, split here
	KafkaEventsTopic string
	KafkaDLQTopic    string
	KafkaGroupID     string

	// Postgres
	PostgresDSN string

	// Redis
	RedisAddr     string
	RedisPassword string

	// Retry policy (see internal/retry) -- the single source of truth for
	// every retry-related number, so nothing is hard-coded in
	// internal/delivery or cmd/worker.
	RetryPolicy retry.Policy

	// ReclaimInterval is how often the worker scans for retryable-and-due
	// or stale-pending rows to reclaim (see internal/delivery.ReclaimSweep).
	ReclaimInterval time.Duration
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
		KafkaDLQTopic:    getEnv("KAFKA_DLQ_TOPIC", "notifications.events.dlq"),
		KafkaGroupID:     getEnv("KAFKA_GROUP_ID", "notification-workers"),

		PostgresDSN: getEnv("POSTGRES_DSN", "postgres://notify:notify@localhost:5433/notify?sslmode=disable"),

		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),

		RetryPolicy: retry.Policy{
			MaxAttempts:       getEnvInt("MAX_ATTEMPTS", 5),
			InitialBackoff:    getEnvDuration("INITIAL_BACKOFF", 2*time.Second),
			MaxBackoff:        getEnvDuration("MAX_BACKOFF", 60*time.Second),
			Jitter:            getEnvFloat("BACKOFF_JITTER", 0.2),
			StaleClaimTimeout: getEnvDuration("STALE_CLAIM_TIMEOUT", 45*time.Second),
		},
		ReclaimInterval: getEnvDuration("RECLAIM_INTERVAL", 5*time.Second),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
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
