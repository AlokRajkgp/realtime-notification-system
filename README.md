# Realtime Event-Driven Notification System

A backend-heavy portfolio project: a REST API accepts "notify this user"
events, publishes them to Kafka partitioned by user ID, and a consumer
group fans them out to channel adapters (in-app, email, and one more) with
idempotent, retried, rate-limited, observable delivery.

## Status

Repo scaffold, local infra, the producer API, a Postgres-backed idempotent
consumer/worker honoring per-user preferences, and one real channel adapter
(in-app, over WebSocket + Redis Pub/Sub). Email/push adapters, rate
limiting, retry/DLQ, metrics, and the frontend don't exist yet. See
"Roadmap" below.

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
  api/       # producer REST API — accepts events, publishes to Kafka, serves the WebSocket
  worker/    # consumer group -> channel adapters
internal/
  adapters/     # one Adapter per delivery channel (in-app so far)
  api/          # Gin router + handlers
  config/       # env var loading
  db/           # Postgres store: delivery_status, preferences, DND — claim/mark/query
  kafkaclient/  # Kafka producer/consumer wrappers
  models/       # shared structs
  preferences/  # combines opt-in/out + DND into one Allowed() check (worker-only)
  ws/           # WebSocket hub for the in-app channel
migrations/  # golang-migrate .sql files
tools/ws-test.html  # throwaway browser page for eyeballing the in-app channel
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

### Watching the in-app channel live

Open `tools/ws-test.html` directly in a browser (with `make up`/`run-api`/
`run-worker` running), click connect with `user_id=u1`, then POST an event
for `u1` as above — the event JSON appears in the page immediately. Under
the hood: the worker's `in-app` adapter (`internal/adapters`) publishes to a
per-user Redis Pub/Sub channel, and whichever API instance holds that
user's WebSocket (`internal/ws`) forwards it to the browser. If nobody's
connected when the event is delivered, Redis Pub/Sub simply has zero
subscribers — the in-app channel intentionally doesn't queue for offline
users (email/push will, once built).

### Preferences and DND

```
# Opt a user out of a channel (any channel not listed here is implicitly enabled)
curl -X PUT localhost:8080/api/v1/users/u2/preferences/in-app -d '{"enabled":false}'

# Set a quiet-hours window (End before Start means it wraps past midnight)
curl -X PUT localhost:8080/api/v1/users/u2/dnd -d '{"start":"22:00","end":"07:00","timezone":"Asia/Kolkata"}'

curl localhost:8080/api/v1/users/u2/preferences   # see current channels + dnd
curl -X DELETE localhost:8080/api/v1/users/u2/dnd  # remove quiet hours
```

The worker checks both before ever calling an adapter: opted-out or inside
a DND window records `delivery_status.status = "skipped"` (not "sent" or
"failed") — same idempotent claim as everything else, so a redelivered
Kafka message can't cause a duplicate skip entry either.

## Roadmap

- [x] Repo scaffold, docker-compose (Postgres/Redis/Redpanda), producer API
- [x] Postgres schema: delivery_status (doubles as the dedupe/idempotency table)
- [x] Consumer group + idempotent delivery claiming
- [x] Delivery status query API (`GET /api/v1/events/:event_id/status`)
- [x] In-app channel adapter: WebSocket + Redis Pub/Sub fan-out
- [x] User preferences: per-channel opt-in/out + DND windows
- [ ] Email + one more channel adapter (push or SMS stub)
- [ ] Retry with backoff + Dead Letter Queue + admin replay endpoint
- [ ] Per-user rate limiting (Redis token bucket)
- [ ] Prometheus /metrics + Grafana dashboard
- [ ] Minimal React frontend
- [ ] Load testing (k6/Locust) + benchmarks
- [ ] Free-tier deploy (Render/Fly.io, Neon/Supabase, Upstash, Vercel)
