.PHONY: up down logs run-api run-worker build migrate-up migrate-down

## Start Postgres, Redis, Redpanda (+ console) in the background.
up:
	docker compose up -d

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
