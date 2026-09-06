# Migrations

Plain SQL migrations, run with the [golang-migrate](https://github.com/golang-migrate/migrate) CLI
(`brew install golang-migrate`, or `make migrate-up` / `make migrate-down`).

None yet — the first migration (users, preferences, delivery_status, dedupe
tables) lands in the step where we build out the DB schema.

To create a new migration pair once we get there:

```
migrate create -ext sql -dir migrations -seq <name>
```
