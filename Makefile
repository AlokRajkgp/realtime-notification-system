.PHONY: up down logs run-api run-worker build migrate-up migrate-down kafka-topic test

## Start Postgres, Redis, Redpanda (+ console) in the background.
up:
	docker compose up -d

## Create the notifications.events topic with a fixed partition count
## *before* anything produces to it. Redpanda auto-creates a topic on
## first publish, but with only 1 partition by default -- that silently
## caps the consumer group at 1 useful worker no matter how many you run.
## Run this once after `make up` on a fresh environment (safe to re-run;
## it's a no-op if the topic already exists with the right partition count).
kafka-topic:
	docker compose exec -T redpanda rpk topic create notifications.events --partitions 3 --replicas 1 2>&1 | grep -v "TOPIC_ALREADY_EXISTS" || true
	docker compose exec -T redpanda rpk topic create notifications.events.dlq --partitions 3 --replicas 1 2>&1 | grep -v "TOPIC_ALREADY_EXISTS" || true

## Run the delivery-state/retry/DLQ tests (internal/delivery). Requires
## Postgres up and migrations applied.
test:
	go test ./... -v -count=1

## Stop and remove the local infra containers (data volumes kept).
down:
	docker compose down

## Tail logs from all local infra containers.
logs:
	docker compose logs -f

## Run the producer API against whatever's in .env (needs `make up` first).
run-api:
	go run ./cmd/api

## Run the consumer worker (placeholder for now).
run-worker:
	go run ./cmd/worker

## Compile both binaries into ./bin.
build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker

## Apply all up migrations. Requires the golang-migrate CLI:
##   brew install golang-migrate
migrate-up:
	migrate -database "$${POSTGRES_DSN:-postgres://notify:notify@localhost:5433/notify?sslmode=disable}" -path migrations up

## Roll back the last migration.
migrate-down:
	migrate -database "$${POSTGRES_DSN:-postgres://notify:notify@localhost:5433/notify?sslmode=disable}" -path migrations down 1
