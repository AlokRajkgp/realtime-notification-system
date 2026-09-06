# Realtime Event-Driven Notification System

A backend-heavy portfolio project: a REST API accepts "notify this user"
events, publishes them to Kafka partitioned by user ID, and a consumer
group fans them out to channel adapters (in-app, email, and one more) with
idempotent, retried, rate-limited, observable delivery.

## Status

Step 1 of the build — repo scaffold, local infra, and the producer API only.
Nothing downstream of Kafka exists yet (no consumer, no DB schema, no
channel adapters, no frontend). See "Roadmap" below.

## Stack

| Concern | Choice | Why |
|---|---|---|
| Language | Go | |
| HTTP | [Gin](https://github.com/gin-gonic/gin) | |
| Event log | [Redpanda](https://redpanda.com/) locally (Kafka API-compatible, single binary — no ZooKeeper/JVM) | Code talks plain Kafka wire protocol, so swapping in real Kafka or a hosted Kafka-compatible service later is a config change, not a code change |
| Kafka client | [segmentio/kafka-go](https://github.com/segmentio/kafka-go) | Pure Go, no cgo — simple to build/cross-compile for free-tier hosts |
| Database | Postgres, via [sqlx](https://github.com/jmoiron/sqlx) + [pgx](https://github.com/jackc/pgx) | Plain SQL, no ORM |
| Migrations | [golang-migrate](https://github.com/golang-migrate/migrate) | Explicit `.sql` files |
| Cache/rate-limit | Redis | |

## Repo layout

```
cmd/
  api/       # producer REST API — accepts events, publishes to Kafka
  worker/    # consumer group -> channel adapters (placeholder for now)
internal/
  api/       # Gin router + handlers
  config/    # env var loading
  kafkaclient/  # Kafka producer/consumer wrappers
  models/    # shared structs
migrations/  # golang-migrate .sql files
docker-compose.yml  # Postgres + Redis + Redpanda (+ console) for local dev
```

## Running locally

```
cp .env.example .env
make up            # starts Postgres, Redis, Redpanda, Redpanda Console (localhost:8081)
make migrate-up     # creates the delivery_status table
make run-api        # producer API on :8080
make run-worker      # consumer group — run in a second terminal (start 2-3 for a real "group")
```

Send a test event:

```
curl -i -X POST localhost:8080/api/v1/events \
  -H 'content-type: application/json' \
  -d '{"user_id":"u1","type":"order.shipped","payload":{"order_id":"o123"}}'
```

A `202 Accepted` with an `event_id` means it reached Kafka — you can also
watch it land in the `notifications.events` topic at `localhost:8081`
(Redpanda Console). A moment later, the worker log shows it being
"delivered", and:

```
curl localhost:8080/api/v1/events/<event_id>/status
```

returns the per-channel delivery outcome from Postgres. POST the same
`event_id` twice (or restart the worker before it commits) and the worker
logs "duplicate delivery skipped" instead of a second delivery — that's the
`delivery_status` unique constraint doing its job.

## Roadmap

- [x] Repo scaffold, docker-compose (Postgres/Redis/Redpanda), producer API
- [x] Postgres schema: delivery_status (doubles as the dedupe/idempotency table)
- [x] Consumer group + idempotent delivery claiming (channel adapters are still a log line, not real sends)
- [x] Delivery status query API (`GET /api/v1/events/:event_id/status`)
- [ ] Real channel adapters (in-app WS/SSE, email, push/SMS)
- [ ] User preferences table + opt-in/out + DND windows (worker currently sends every event to every channel)
- [ ] Retry with backoff + Dead Letter Queue + admin replay endpoint
- [ ] Per-user rate limiting (Redis token bucket)
- [ ] Prometheus /metrics + Grafana dashboard
- [ ] Minimal React frontend
- [ ] Load testing (k6/Locust) + benchmarks
- [ ] Free-tier deploy (Render/Fly.io, Neon/Supabase, Upstash, Vercel)
