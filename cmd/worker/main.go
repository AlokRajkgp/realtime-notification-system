// Command worker will run the consumer group that reads from Kafka and
// routes events to channel adapters (in-app/email/push). Not implemented
// yet — this is a placeholder so the repo layout is visible from step one
// and `go build ./...` succeeds; it's built out in a later step.
package main

import "log"

func main() {
	log.Println("worker: not implemented yet — see cmd/api for the producer API")
}
