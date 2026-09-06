# Interview Notes — Realtime Event-Driven Notification System

Personal interview-prep document. Everything here describes what is
**actually implemented and verified** in this repo as of Phase C. Where
something isn't built yet, it's labeled explicitly:
**"Not implemented in the current version."** or **"Future improvement —
not currently implemented."** Nothing here is aspirational.

---

## 1. Project overview

**Problem being solved:** accept a "notify this user" event over HTTP, and
reliably get it to the user through one or more channels (today: in-app,
over WebSocket), even if a downstream system is briefly broken, without
ever silently losing or duplicating a delivery.

**Why this architecture:** decouple "accepting the event" from "delivering
it." A REST API that tried to deliver synchronously would fail the whole
request if a channel adapter (email, push, in-app) was slow or down.
Instead, the API's only job is to durably record the event (Kafka) and
return immediately; a separate consumer does the actual delivery work,
independently, with its own retry/failure handling.

### High-level architecture

```
                                   ┌──────────────┐
   client ── POST /api/v1/events ─▶│  api (Gin)   │
                                   └──────┬───────┘
                                          │ publish, keyed by user_id
                                          ▼
                              ┌───────────────────────┐
                              │ Kafka topic:           │
                              │ notifications.events   │
                              │ (3 partitions)          │
                              └───────────┬────────────┘
                                          │ consumer group: notification-workers
                                          ▼
                                   ┌──────────────┐
                                   │ worker        │
                                   │ (internal/    │
                                   │  delivery)    │
                                   └───┬───────┬───┘
                     claim/mark state  │       │ Send()
                     in Postgres       ▼       ▼
                              ┌────────────┐ ┌────────────────┐
                              │ delivery_  │ │ in-app adapter  │
                              │ status     │ │ -> Redis Pub/Sub│
                              └────────────┘ └────────┬────────┘
                                                       │ notify:inapp:<user_id>
                                                       ▼
                                              ┌──────────────────┐
                                              │ api instance      │
                                              │ holding user's WS │
                                              └────────┬─────────┘
                                                       ▼
                                                    browser

  permanently failed / retries exhausted:
      worker ──▶ Kafka topic notifications.events.dlq
```

**Request flow (synchronous):** `POST /api/v1/events` → validate → publish
to Kafka (blocks until the broker acks) → `202 Accepted` with the
`event_id`. The API never waits for delivery.

**Async processing flow:** worker consumes the Kafka message → claims the
delivery in Postgres → checks preferences/DND → calls the adapter → records
the outcome. If the send fails transiently, a background reclaim-scan
retries it later with backoff, entirely through Postgres — Kafka is not
involved again after the first attempt.

**Kafka's role:** durable, ordered (per user) ingestion of events, and the
mechanism that lets multiple worker processes split the work (a consumer
group).

**PostgreSQL's role:** the source of truth for delivery state — the
idempotency mechanism, the retry/backoff state machine, and user
preferences/DND. Not just a log of what happened; it's what *drives* what
happens next.

**Redis's role:** exactly one thing — Pub/Sub fan-out so a worker process
(which has no direct connection to any browser) can hand a message to
whichever API instance holds that user's WebSocket. **Not** a durable
queue, **not** a cache in this project (see section 10).

**WebSocket's role:** the in-app notification "bell" — a live push channel
to a connected browser.

**Scaling model:** more `api` instances scale HTTP + WebSocket handling
(they're stateless — no code changes needed, no coordination between
them). More `worker` instances scale delivery processing, up to the Kafka
partition count (3 today).

### 30-second explanation
"It's an event-driven notification system: a REST API publishes events to
Kafka, a worker consumes them and delivers to channels like an in-app
WebSocket bell, and Postgres tracks delivery state with retries and a dead
letter queue for anything that keeps failing. Kafka gives at-least-once
delivery; Postgres is what makes repeated delivery attempts safe."

### 2-minute explanation
"A client posts an event — say, 'notify user X that their order shipped'
— to a Gin REST API, which publishes it to a Kafka topic keyed by user ID,
so one user's events stay ordered and go to the same partition. A worker
process, in a Kafka consumer group, picks it up and tries to deliver it —
today, over a WebSocket connection via Redis Pub/Sub, since the browser
connection might be held by a different API instance than the one that
published the event. Every delivery attempt is claimed in Postgres with a
unique constraint before anything is sent, so a redelivered Kafka message
never gets sent twice. If a send fails and the failure looks transient —
like Redis being briefly down — it gets scheduled for a retry with
exponential backoff. If it keeps failing, or the failure is clearly
permanent, it gets dead-lettered to a separate Kafka topic and marked dead
in Postgres, so it's visible and doesn't retry forever. I also handle the
case where a worker crashes mid-delivery: a background scan reclaims any
delivery that's been 'claimed' for too long without resolving, using
Postgres's own row-locking so two workers can't race on the same reclaim."

### 5-minute explanation
Everything above, plus: the API and worker are two separate Go binaries
sharing internal packages; the API is stateless (any instance can serve
any request) except that a WebSocket connection lives on whichever
instance accepted it, which is why in-app delivery goes through Redis
Pub/Sub rather than an in-process registry. The Kafka topic runs with 3
partitions, which is the real ceiling on how many worker processes can do
useful work at once — verified by running 3 workers and confirming via
`rpk group describe` that each owned exactly one partition. Both binaries
handle SIGTERM gracefully: the API drains in-flight HTTP requests (though
not open WebSockets — a documented gap), and the worker finishes whatever
delivery it's mid-processing before exiting, using two separate contexts
so the shutdown signal can't abort a half-done database write. The whole
system targets "at-least-once processing with idempotent application
behavior," not exactly-once — that distinction, and exactly why, is in
section 4.

---

## 2. Phase B concepts

### Kafka topic
**Simple explanation:** a named, append-only log. Producers append,
consumers read; nothing is deleted when read.
**This project:** `notifications.events` (events in) and
`notifications.events.dlq` (permanently failed deliveries).
**Interview question:** "What's the difference between a Kafka topic and a
queue like RabbitMQ?"
**Strong answer:** "A queue typically removes a message once it's
consumed. A Kafka topic keeps messages for a retention window regardless
of consumption, and multiple independent consumer groups can each read the
same topic at their own pace without affecting each other."
**Common misconception:** that reading a message removes it — it doesn't;
only the consumer's committed offset advances.

### Partition
**Simple explanation:** a topic is split into independent, ordered logs
called partitions. A partition can be read by at most one consumer per
consumer group at a time — this is the actual unit of parallelism.
**This project:** `notifications.events` has 3 partitions (was 1, found
during the Phase A audit — a real bug, not a design choice, since Redpanda
auto-creates topics with 1 partition by default).
**Interview question:** "You have 3 partitions and 5 consumer processes in
one group. What happens?"
**Strong answer:** "3 of them get assigned one partition each and do real
work; the other 2 sit idle as standbys, and only become active if one of
the 3 dies and a rebalance reassigns its partition."
**Common misconception:** that adding more consumer processes always adds
throughput — it doesn't, past the partition count.

### Key
**Simple explanation:** an optional value attached to a message that Kafka
hashes to deterministically choose a partition — same key, same partition,
every time (as long as the partition count doesn't change).
**This project:** `user_id` is the key, so one user's events are always
processed in order (by whichever single consumer owns that partition).
**Interview question:** "Why key by `user_id` instead of `event_id`?"
**Strong answer:** "Keying by `event_id` would spread even one user's
events randomly across partitions, so they could be processed out of
order by different consumers at the same time. Keying by `user_id` keeps
one user's events on one partition, so they're processed in the order
they were published."
**Common misconception:** that a key guarantees *global* ordering — it
only guarantees ordering *within* the set of messages sharing that key.

### Offset
**Simple explanation:** a strictly increasing position of a message within
one partition. A consumer's entire "progress" is just "which offset have I
committed, per partition."
**This project:** the worker calls `FetchMessage` then explicitly
`Commit`s — not the auto-commit `ReadMessage` — so the offset only
advances once a message has been fully claimed in Postgres.
**Interview question:** "What's the difference between fetching a message
and committing its offset?"
**Strong answer:** "Fetching gets you the message; committing tells the
broker you're done with it. If you crash between the two, the broker still
thinks you haven't processed it, and will redeliver it to whoever picks up
that partition next."
**Common misconception:** conflating "read" with "processed" — a message
can be fetched and then lost (crash) without ever being committed.

### Producer
**Simple explanation:** anything that writes messages to a topic.
**This project:** `internal/kafkaclient.Producer`, used by the API (to
publish events) and the worker (to publish DLQ records) — same type, two
different topics.
**Interview question:** "Why does the DLQ use the same Producer type as
the main event publish path?"
**Strong answer:** "It's the same operation — publish a JSON value to a
topic, keyed the same way. Building a second abstraction for it would be
duplicated code for no behavioral difference."

### Consumer
**Simple explanation:** one reader of a topic — one `kafka.Reader`
instance in this codebase.
**This project:** one per worker process.

### Consumer group
**Simple explanation:** a named set of consumers that split a topic's
partitions between them — the broker guarantees each partition goes to
exactly one consumer in the group at a time.
**This project:** `notification-workers` — every worker process joins it.
**Interview question:** "How do multiple worker processes coordinate who
processes what?"
**Strong answer:** "They don't coordinate with each other at all — the
Kafka broker does it, by assigning partitions to consumer group members
and rebalancing when membership changes."

### Rebalancing
**Simple explanation:** when group membership changes (a consumer joins,
leaves, or is judged dead), the broker recomputes and reassigns partitions.
**This project:** observed live — killing an orphaned worker process
caused the group to drop from 4 members to 3, and a fresh `rpk group
describe` a few seconds later showed a clean 3-way assignment.
**Interview question:** "What happens to a message a consumer had fetched
but not yet committed, right as a rebalance happens?"
**Strong answer:** "It gets redelivered to whichever consumer ends up
owning that partition after the rebalance — which is exactly why delivery
has to be idempotent, not just 'trust that I've already seen this.'"

### Why 3 partitions
Chosen to match "run 2-3 worker instances" from the original project
scope — it's a capacity-planning number, not a magic constant. More
partitions than planned-for workers just means idle capacity; fewer would
under-utilize the workers you actually run.

### Why `user_id` as the key
Covered under "Key" above — it's the ordering guarantee: this system's one
ordering promise is "one user's events are processed in the order they
were published," and that's implemented entirely by this key choice, not
by any code in the worker.

### Consumer-group parallelism
The partition count is a hard ceiling on how many consumers in one group
can do useful work simultaneously. Verified live: 3 worker processes, 3
partitions, `rpk group describe` showing one partition per worker, 6 test
users processed with overlapping (parallel, not sequential) timestamps.

### Why we did NOT introduce a worker pool
The worker processes one Kafka message at a time, sequentially, in one
goroutine. Adding intra-process concurrency (a pool of goroutines each
processing a different message from the same partition) would require
solving a real problem this design avoids for free: Kafka only lets you
commit one offset per partition, so processing messages out of order
within a partition means you can't safely commit past a message that's
still in flight without risking losing it on a crash. Partition-based
scaling (more worker *processes*) is simpler, correctly ordered, and
we hadn't measured (via load testing — not done yet, a later phase) that a
single sequential worker was even the bottleneck. Premature complexity
avoided on purpose.

### Graceful shutdown
**Simple explanation:** handling SIGINT/SIGTERM by finishing current work
and cleaning up, instead of being killed mid-operation.
**This project:** `cmd/api` uses `http.Server.Shutdown` with a 10s grace
period for in-flight HTTP requests (does **not** wait for open WebSocket
connections — a documented gap, verified live: a SIGTERM with an open `/ws`
connection produced an immediate `close 1006 unexpected EOF` on the
client). `cmd/worker` stops fetching new Kafka messages immediately but
lets an in-hand message finish processing first.
**Interview question:** "Why does graceful shutdown matter here
specifically, beyond 'it's good practice'?"
**Strong answer:** "Because an ungraceful kill mid-delivery looks
identical, from the system's point of view, to a real crash — it leaves a
delivery_status row claimed but unresolved. Every deploy or restart would
otherwise be indistinguishable from a failure, which is exactly the
scenario the stale-claim recovery mechanism exists to handle — but it's
much better to avoid causing that scenario unnecessarily on every routine
restart."

### `signal.NotifyContext`
**Simple explanation:** a stdlib helper (Go 1.16+) that returns a
`context.Context` which is cancelled the moment a specified OS signal
arrives — built on `os/signal.Notify` + `context.WithCancel` under the
hood, not a separate mechanism.
**This project:** used in both `cmd/api` and `cmd/worker` to get a
cancellable root context from SIGINT/SIGTERM with no extra plumbing.

### Two-context shutdown design
**Simple explanation:** using one context to mean two different things
("stop waiting for new work" and "abort work already in progress") is a
common mistake — they need to be separate.
**This project:** the worker's `rootCtx` (cancelled on signal) is passed
*only* to the blocking `FetchMessage` call. An already-fetched message is
processed under a completely separate `context.WithTimeout(context.
Background(), 15s)`, immune to the shutdown signal. Verified live: sent
SIGTERM mid-way through a 20-event burst; the worker's log showed it
finishing delivery of the last event *before* exiting, and Postgres showed
zero events stuck at `pending`.
**Interview question:** "Why not just use one context everywhere for
shutdown?"
**Strong answer:** "Because pgx and go-redis both honor context
cancellation — if the same context used to signal shutdown were passed
into an in-flight database write, cancelling it on SIGTERM would abort
that write mid-flight. That's worse than not shutting down gracefully at
all: it would manufacture the exact 'crashed mid-processing, stuck
pending row' failure this project's retry design exists to recover from,
except self-inflicted on every routine deploy."

### WebSocket ping/pong
**Simple explanation:** an application-level heartbeat — the server sends
a Ping control frame; a healthy client's WebSocket implementation answers
with a Pong automatically, with no page code involved.
**This project:** ping every 15s (`pingPeriod`), pong expected within 20s
(`pongWait`).
**Interview question:** "Why isn't TCP's own connection state enough to
detect a dead client?"
**Strong answer:** covered in detail in section 11 — short version: TCP
can report "connected" even when the other end's application has frozen
or the machine went to sleep without a clean close.

### Read/write deadlines
**Simple explanation:** a deadline that makes a blocking read or write
call return an error if it doesn't complete in time, instead of blocking
forever.
**This project:** `SetReadDeadline(pongWait)` on connect, refreshed to
`now + pongWait` every time a pong is received via `SetPongHandler`; if no
pong arrives in time, the blocked `ReadMessage` call in the reader
goroutine times out — reusing the exact same "read error → cancel()"
cleanup path that already existed for a normal disconnect. `SetWriteDeadline`
(5s) bounds how long a single ping or message write may take.

### TCP connection vs. application-level liveness
**Simple explanation:** the OS reporting a TCP connection as `ESTABLISHED`
only means the socket hasn't been torn down — it says nothing about
whether the process on the other end is actually running, reading, or
responsive.
**This project:** verified directly. A test client that connected and then
never called `ReadMessage` again (simulating a frozen tab) kept its socket
`ESTABLISHED` (confirmed via `lsof`) the entire time, while the server-side
ping/pong mechanism detected and closed the connection in exactly 20
seconds — the socket then observed transitioning to `CLOSE_WAIT`.
**Interview question:** "A monitoring dashboard shows a TCP connection as
healthy. Is the client necessarily receiving your messages?"
**Strong answer:** "Not necessarily — TCP-level health and application-
level liveness are different things, which is exactly why protocols like
WebSocket define an application-level heartbeat (ping/pong) instead of
relying on TCP's own keepalive."

---

## 3. Reliable delivery

### State machine

```
                    ClaimNew() [Kafka message, first sight]
                              │
                              ▼
                        ┌───────────┐
              ┌────────▶│  pending  │◀────────┐
              │         └─────┬─────┘         │
              │      claimed_at = lease        │ ReclaimDue()
              │               │                │ (stale pending OR
   preferences│      adapter.Send()            │  retryable past due)
   disallow   │               │                │
              │        ┌──────┼──────┐         │
              ▼        ▼      │      ▼         │
        ┌──────────┐ ┌─────┐  │  ┌─────────────┴┐
        │ skipped  │ │sent │  │  │  retryable   │
        └──────────┘ └─────┘  │  │(next_attempt_│
         (terminal)  (terminal)  │  at = backoff)│
                              │  └──────────────┘
                    attempts >= MaxAttempts
                        OR permanent error
                              │
                              ▼
                        ┌───────────┐
                        │   dead    │  (DLQ)
                        └───────────┘
                         (terminal)
```

| State | Meaning | Set by |
|---|---|---|
| `pending` | Currently claimed; a worker is (or was) actively attempting it. Carries `claimed_at`, a fencing token/lease. | `ClaimNew` or `ReclaimDue` |
| `retryable` | Last attempt failed transiently; unowned; eligible for reclaim once `next_attempt_at` passes | worker, on transient `Send()` failure with attempts remaining |
| `sent` | Terminal success | worker, `Send()` returned nil |
| `skipped` | Terminal — preferences/DND blocked it | worker, before calling `Send()` |
| `dead` | Terminal — permanent error or retries exhausted; this **is** the DLQ marker | worker, permanent error or `attempts >= MaxAttempts` |

**Attempts:** one attempt = one claim = one complete processing pass
(preference check + one `Send()` call + one mark). `attempts` is
incremented atomically as part of the same `UPDATE` that performs the
(re)claim.

**Retry policy** (`internal/retry.Policy`, centralized, injected — no
magic numbers scattered around):
- `MaxAttempts` (default 5): total tries before giving up.
- `InitialBackoff` (default 2s): delay before attempt 2.
- `MaxBackoff` (default 60s): hard cap on delay, however high attempts get.
- `Jitter` (default 0.2 = ±20%): randomizes the delay around the
  exponential value, so deliveries that failed at the same instant (e.g. a
  shared Redis blip) don't all retry simultaneously and hit it again at
  once (a "thundering herd").
- Formula: `delay = exponential(InitialBackoff, attempt)`, capped at
  `MaxBackoff`, then randomized by `±Jitter` fraction.
- `StaleClaimTimeout` (default 45s): how long a `pending` row may sit
  unresolved before it's treated as abandoned. Must exceed how long one
  delivery attempt can legitimately take (the worker's 15s per-message
  processing bound), or a still-in-progress claim would be reclaimed out
  from under its owner.

**Stale claims:** `claimed_at` is not just a timestamp — it's a fencing
token. Every `Mark*` call must present the exact `claimed_at` it was
issued; if another worker has since reclaimed the row, the write is
silently discarded rather than corrupting newer state (see section 6).

**DLQ:** on permanent failure or exhausted retries, the original event
plus failure context (attempts, final error, timestamp) is published to
`notifications.events.dlq` (a real Kafka topic, not just a status flag),
then the row is marked `dead`.

**A real debugging story worth knowing (good "hard bug" interview
material):** while writing the tests for this state machine, several of
them intermittently failed with a symptom that looked like `attempts`
jumping by more than one per reclaim. Root-causing it required ruling out,
in order: a genuine logic bug (disproven — two independent standalone
reproductions of the identical scenario, outside the test framework,
always passed); a client/server clock-skew bug (real, and fixed —
`MarkRetryable` was computing `next_attempt_at` using the Go process's
clock and comparing it against Postgres's own `now()` later, which is
unsafe across a container boundary; fixed by having Postgres compute the
deadline itself, the same way the stale-claim check already did); and
finally the actual cause — the reclaim scan's query is *correctly* global
(it has to scan the whole table, not just one test's row), so on a
memory-pressured host running a long-lived VM, one test hitting a genuine
scheduling stall could leave its row non-terminal, which then inflated the
reclaim *count* (not the correctness) seen by whichever test ran next. The
fix wasn't a database change at all — it was making the tests assert on
each test's own row state (always correctly scoped, since `GetByEventID`
filters by `event_id`) instead of on the shared global count, and polling
for the expected state instead of a single fixed sleep-then-check. The
lesson: a flaky integration test around a deliberately-global query is
often a test-assertion-scope problem, not a concurrency bug in the
production code — but you have to actually rule out the production code
first, not assume it.

---

## 4. Kafka delivery semantics

- **At-most-once:** a message might be processed zero or one times — never
  more. Achieved by committing the offset *before* processing (this
  project does not do this).
- **At-least-once:** a message might be processed one or more times —
  never zero. Achieved by committing the offset *after* processing
  succeeds. **This project's design.**
- **Exactly-once:** a message is processed exactly one time, guaranteed.
  Requires either a single transactional system spanning the broker and
  the side effect (Kafka's transactional producer/consumer APIs can give
  you this *within Kafka*, e.g. consume-transform-produce), or an
  idempotent side effect combined with at-least-once delivery.

**What this system actually provides: at-least-once processing, made safe
by idempotent application behavior** — not exactly-once delivery. The
Postgres unique constraint on `(event_id, channel)` ensures a redelivered
Kafka message, or a redundant reclaim, never results in two `Send()`
calls landing (see section 5 for exactly what it does and doesn't cover).

**Why not just claim exactly-once:** the external side effect
(`adapter.Send`, e.g. a Redis publish) is not part of Kafka or Postgres's
transaction boundary. Nothing can make "commit this Kafka offset," "write
this Postgres row," and "publish this Redis message" atomic across three
separate systems. The honest claim is that duplicate *processing* is
prevented (the unique constraint), not that duplicate *external delivery*
is provably impossible in every failure window (see the table below).

### Failure matrix (reflects this implementation exactly)

| Failure point | What happens | Duplicate possible? | Recovery |
|---|---|---|---|
| Before `Send` (crash right after claim) | Row stuck at `pending` | No — nothing was ever sent | `ReclaimDue` reclaims it once `StaleClaimTimeout` elapses; re-attempted from scratch |
| During `Send` (crash while the network call is in flight) | Row stuck at `pending`; whether the external side actually received it is unknown | **Possible** — if the send actually landed before the crash, the reclaim will send it again | `ReclaimDue` reclaims it; a second `Send` may duplicate the external delivery (see below) |
| After `Send` succeeds, before `MarkSent` | Row stuck at `pending`; delivery genuinely happened once | **Possible** — reclaim will call `Send` again, believing it never happened | Same as above — the row eventually resolves to `sent`, but at the cost of a possible duplicate external delivery |
| After `MarkSent`/`MarkRetryable`/`MarkDead`, before Kafka `Commit` | Kafka redelivers the same message | **No** — `ClaimNew` sees the row already exists (`claimed=false`) and does nothing further | Handled entirely by the existing unique-constraint dedupe; verified in `TestHandleNew_RedeliveredBeforeCommit_NotLostNotDuplicated` |
| `Commit` itself fails (network blip to the broker) | Offset not advanced; message will be redelivered even though it was fully processed | **No** — same as above, the dedupe check absorbs it | Same as above |
| Kafka redelivers for any reason (rebalance, restart, retry) | `ClaimNew` returns `claimed=false` if a row exists in any state | No duplicate *processing* | No-op; whatever the row's current state is stands |
| Stale claim reclaimed (worker A slow, worker B reclaims and finishes first) | A's eventual `Mark*` call is discarded (fencing token mismatch) | No duplicate *recorded state* — but see below | Covered in section 6 |

**The one honest duplicate-delivery window this system does not close:**
if a worker crashes *between* a successful external `Send()` and the
`MarkSent` call that would have recorded it, the row is still `pending`,
and it looks identical — from Postgres's point of view — to "never sent."
The reclaim scan will retry it, calling `Send()` a second time. For the
in-app channel (Redis Pub/Sub), the practical impact of "the bell rings
twice" is minor. **This is the honest limit of the design: idempotent
*processing* (never send twice for the same claim) is guaranteed; idempotent
*external delivery* in this narrow crash window is not**, because nothing
can make an external network call and a database write atomic with each
other.

### Concrete example: `event_id = evt_123`
1. API publishes `evt_123` for `user_42`, keyed by `user_42` → lands on
   partition 1.
2. Worker A fetches it, calls `ClaimNew(evt_123, "in-app")` → succeeds,
   row created, `status=pending`, `attempts=1`.
3. Worker A calls the in-app adapter's `Send()` — it successfully
   publishes to Redis.
4. Worker A crashes right here, before `MarkSent` runs.
5. Kafka's offset for `evt_123` was never committed (step 2 succeeded but
   the commit happens after all adapters finish) — but that doesn't matter
   for *this* adapter's row, because the row already exists.
6. `StaleClaimTimeout` (45s) elapses. The reclaim scan picks up the
   `pending` row, bumps `attempts` to 2, calls `Send()` again.
7. Result: `evt_123` was delivered to the in-app channel **twice** for
   this one adapter — the honest gap above, playing out concretely.
8. If instead Worker A had crashed *before* step 3 (before `Send` was ever
   called), step 6's retry would be the *first* real delivery — correct,
   no duplicate.

---

## 5. Idempotency

**What it means here:** processing the same event more than once produces
the same recorded outcome as processing it once — no duplicate rows, no
double-counting.

**Why needed:** Kafka is at-least-once by design; without idempotency,
every redelivery (crash, rebalance, restart) would mean a duplicate
notification.

**The Postgres unique constraint — what it protects:** `UNIQUE(event_id,
channel)` on `delivery_status`, enforced via `INSERT ... ON CONFLICT DO
NOTHING`. It guarantees that only one row can ever exist per (event,
channel) pair, so only one "claim" can ever succeed for it. This is what
makes duplicate *processing* — a second worker trying to independently
process the same (event, channel) — safe and cheap to detect (the second
`ClaimNew` just returns "already exists").

**What it does NOT protect:** the constraint says nothing about the
external side effect (a Redis publish, an email send). It cannot make
"the row was inserted" and "the message was delivered" a single atomic
operation — those happen in two different systems, one of which (an
external channel) doesn't participate in Postgres transactions at all.
See the `evt_123` example above for the exact window where this matters.

**How Kafka redelivery interacts with it:** every redelivered message goes
through the exact same `ClaimNew` call as the original. If a row already
exists in *any* state (`pending`, `retryable`, `sent`, `skipped`, `dead`),
the redelivery is a no-op at the claim level — nothing about Kafka
redelivery on its own can cause a duplicate `Send()` call. Duplicate
`Send()` calls only happen through the reclaim path (stale or retryable
rows), never through raw Kafka redelivery.

---

## 6. Failure scenarios

| # | Scenario | What happens | Why | Recovery | Duplicate possible? |
|---|---|---|---|---|---|
| 1 | Worker crashes before `ClaimNew` | Nothing recorded anywhere | The message was fetched but never even reached Postgres | Kafka offset was never committed; message redelivered on restart/rebalance | No |
| 2 | Worker crashes after `ClaimNew`, before `Send` | Row stuck at `pending` | Claim succeeded; nothing else ran | Reclaim scan, after `StaleClaimTimeout` | No |
| 3 | Worker crashes during `adapter.Send()` | Row stuck at `pending`; external state unknown | The network call was interrupted | Reclaim scan retries | **Possible**, if the send actually landed before the crash |
| 4 | Worker crashes after `Send()` succeeds | Row stuck at `pending` — looks identical to case 3 from Postgres's view | Same reasoning as the failure matrix above | Reclaim scan retries | **Yes** — this is the one honest gap (section 4) |
| 5 | Worker crashes after `MarkStatus` (sent/retryable/dead), before Kafka `Commit` | Kafka redelivers; `ClaimNew` sees the row, no-ops | The dedupe check runs before anything else on redelivery | Automatic, no action needed | No |
| 6 | Kafka `Commit` call itself fails | Same as case 5 | Offset never advanced | Automatic | No |
| 7 | Kafka temporarily unavailable | `FetchMessage` blocks/errors with internal backoff (kafka-go's own reconnect logic, not code we wrote); worker doesn't crash, doesn't busy-loop (verified: 0% CPU during a real outage in Phase A) | kafka-go's Reader retries internally | Self-heals silently once the broker returns; **no visibility into "still retrying" without checking logs or publishing a test event** — a real observability gap, candidate for Phase G metrics | No |
| 8 | PostgreSQL temporarily unavailable | `ClaimNew` returns an error; `HandleNew` returns `commitOK=false`; the Kafka offset is deliberately **not** committed | We can't even record the claim, so we must not tell Kafka we're done | Message redelivered once Postgres is back | No |
| 9 | Redis temporarily unavailable | `adapter.Send()` fails (dial error); classified as transient (default, not wrapped `PermanentError`) → scheduled for retry | This is the exact scenario **verified live** in this phase: stopped Redis, published an event, watched it go `retryable` with the real error message, restarted Redis, watched the reclaim scan retry and succeed | Reclaim scan, once Redis is back and before `MaxAttempts` is exhausted | No (see case 4's caveat if the publish partially landed) |
| 10 | Adapter permanently fails | E.g. a malformed event that can never be JSON-encoded → wrapped in `PermanentError` → dead-lettered on the very first attempt, no retries wasted | Retrying an error that will fail identically every time wastes attempts and delays real diagnosis | DLQ record + `dead` status; no automatic recovery (see section 8's note on replay) | No |
| 11 | Adapter transiently fails | E.g. Redis dial error → default classification (retryable) | Might succeed next time | Scheduled retry with backoff | No |
| 12 | Two workers process the same delivery | Only one wins the `ClaimNew` insert; the other gets `claimed=false` and does nothing | Postgres's unique constraint | Automatic | No |
| 13 | Stale worker races with reclaiming worker | Covered in detail below | Fencing token (`claimed_at`) | Automatic | No — the *recorded* outcome can't be corrupted, though see case 4 for the underlying external-delivery caveat if the stale worker's send had actually landed |

**Interview answer for #13 (the stale-claim race), in detail:** Worker A
claims a row (`claimed_at = t1`). A becomes slow — GC pause, network
hiccup, anything short of a crash. `StaleClaimTimeout` elapses; Worker B's
reclaim scan picks up the row, because Postgres's `UPDATE ... WHERE
status='pending' AND claimed_at < now() - timeout` matches it — this
single statement is safe under concurrent reclaim attempts by construction
(Postgres locks the row during the scan; a second concurrent `UPDATE`
blocks, then re-evaluates its `WHERE` clause against the now-committed
row, which no longer matches). B sets `claimed_at = t2`, processes it, and
marks it `sent`. A eventually "wakes up" and calls `MarkSent` with its
original claim, `claimed_at = t1`. That call's `WHERE id=$1 AND
claimed_at=$2` no longer matches anything (`claimed_at` is now `t2`), so
zero rows are affected — A's write is silently discarded, verified in
`TestStaleClaimRace_OldWorkerCannotOverwriteNewerState`. No distributed
lock was needed; Postgres's row-level locking during a single `UPDATE`
statement provided it for free.

---

## 7. Retry & backoff

- **Transient failure:** might succeed if retried (e.g. Redis unreachable
  for a moment). Default classification for any error an adapter returns.
- **Permanent failure:** will fail identically no matter how many times
  it's retried (e.g. an event that can never be JSON-encoded). Adapters
  opt into this explicitly via `adapters.PermanentError`.
- **Exponential backoff:** each successive retry waits longer than the
  last (`InitialBackoff * 2^(attempt-1)`, capped at `MaxBackoff`) — avoids
  hammering a struggling downstream system with immediate retries right
  when it's least able to handle them.
- **Max attempts:** a hard ceiling (default 5) so a permanently-broken
  delivery doesn't retry forever, consuming resources indefinitely.
- **Max backoff:** a cap (default 60s) so the delay doesn't grow
  unboundedly for a delivery that's been failing for a long time.
- **Jitter:** randomizing the delay (±20% by default) around the
  exponential value.
- **Thundering herd:** the scenario jitter defends against — many
  deliveries failing at the same moment (e.g. Redis going down affects
  every in-flight send simultaneously) would otherwise all be scheduled
  for retry at the *exact same computed delay*, and all hit the recovering
  system again at the exact same instant, potentially knocking it back
  over.
- **Why immediate retry is dangerous:** retrying instantly, with no delay,
  against a system that's failing because it's overloaded makes the
  overload worse, not better — this is the core justification for backoff
  existing at all, not just a nicety.

**Actual configuration used (defaults, all overridable via env — see
`internal/config`):**
```
MAX_ATTEMPTS=5
INITIAL_BACKOFF=2s
MAX_BACKOFF=60s
BACKOFF_JITTER=0.2
STALE_CLAIM_TIMEOUT=45s
RECLAIM_INTERVAL=5s
```

---

## 8. Dead Letter Queue

**What it is:** a place permanently-failed messages go instead of being
silently dropped or retried forever — visible, inspectable, and (in a more
built-out system) replayable.

**Why needed:** without one, a permanent failure either loops forever
(wasting resources, spamming logs/an external system with retries that
can never succeed) or is silently dropped (losing the failure entirely,
with no record it ever happened).

**When a message goes to the DLQ:** immediately, if the adapter returns a
`PermanentError`; otherwise, once `attempts >= MaxAttempts` after a
transient failure.

**What happens after max attempts:** the original event, channel,
attempt count, final error, and failure timestamp are published as a
`models.DeadLetter` JSON record to the `notifications.events.dlq` Kafka
topic (verified live — read back with `rpk topic consume`), and the
`delivery_status` row is marked `dead`.

**Poison messages:** a Kafka message that can't even be JSON-decoded (not
a delivery failure — a fundamentally malformed message) is handled
separately: logged and the offset committed past it, since there's no
`delivery_status` row to route through the DLQ state machine at all (this
predates Phase C and is unchanged).

**How infinite retry is prevented:** `MaxAttempts`, enforced in
`scheduleRetryOrDeadLetter` — once attempts reach the ceiling, the *only*
path is dead-lettering, never another retry schedule. Verified in
`TestReclaimSweep_MaxAttemptsExceeded_DeadLettersAndStops`, which
explicitly sweeps several more times after reaching `dead` and asserts
the adapter is never called again.

**Ordering of the two DLQ-marking steps:** the Kafka publish happens
*before* the Postgres `dead` mark, not after. Reasoning: if the process
crashes between the two, the worst case is the row stays retryable/pending
and gets tried (and DLQ-published) again later — a redundant but safe
outcome. The opposite order risks a row permanently marked `dead` with no
corresponding DLQ record ever published, silently losing the failure
information for good. This is a deliberate choice about which failure
mode is less bad, not a guarantee that this ordering makes the two writes
atomic (it doesn't — no order can, since they're two different systems).

**Replay:** **Not implemented in the current version.** A `dead` row and
its DLQ Kafka record are both fully inspectable (via `GET
/api/v1/events/:event_id/status` and `rpk topic consume`
`notifications.events.dlq`), but there's no admin endpoint to re-inject a
dead-lettered event back into the live pipeline. **Future improvement —
not currently implemented.**

---

## 9. Database design

- **`delivery_status`:** the entire state machine lives in one table —
  `id, event_id, user_id, channel, status, attempts, last_error,
  claimed_at, next_attempt_at, event_json, created_at, updated_at`.
- **`UNIQUE(event_id, channel)`:** the idempotency mechanism (section 5).
- **Indexes:** `idx_delivery_status_event_id`, `idx_delivery_status_user_id`
  (status queries), and two *partial* indexes added in Phase C —
  `idx_delivery_status_retry_due` (`WHERE status='retryable'`) and
  `idx_delivery_status_stale_pending` (`WHERE status='pending'`) — small
  and cheap to maintain since they only cover rows currently in that one
  status, which is exactly what the reclaim scan queries.
- **Transactions:** deliberately, **none were added in this phase.** Every
  state transition (`ClaimNew`, `ReclaimDue`, each `Mark*`) is a single
  `INSERT`/`UPDATE` statement, and Postgres already makes a single
  statement atomic on its own — wrapping it in an explicit `BEGIN/COMMIT`
  would add nothing. We looked for a genuine need for a multi-statement
  transaction in this phase's new logic and didn't find one.
- **Atomic updates:** `ReclaimDue`'s single `UPDATE ... RETURNING` handles
  potentially many rows at once, atomically, using Postgres's own
  row-locking for correctness under concurrent workers — no extra
  application-level locking code.
- **`claimed_at`:** doubles as a timestamp *and* a fencing token/lease —
  see section 6.
- **`attempts`:** now functional (was a dead field before this phase) —
  incremented as part of the same atomic statement that performs each
  (re)claim.
- **State transitions:** all detailed in section 3.
- **Why `Send()` is not inside a DB transaction:** holding a Postgres
  transaction open across an unpredictable external network call would
  hold row locks for that entire duration, blocking other workers/queries
  and risking transaction timeouts — and it still wouldn't make the
  external call atomic with the database write, since the external system
  doesn't participate in Postgres's transaction at all. It would add real
  cost for zero actual correctness benefit.

---

## 10. Redis

**Exactly what Redis does in this project: Pub/Sub fan-out for the in-app
WebSocket channel. Nothing else.** There is no caching layer, no rate
limiting (not implemented yet — future phase), no session storage.

- **Pub/Sub:** the worker's in-app adapter publishes an event's JSON to a
  per-user channel (`notify:inapp:<user_id>`); the API instance holding
  that user's WebSocket is subscribed to the same channel and forwards the
  message.
- **Why Pub/Sub is useful here:** it lets a worker process, which has no
  direct connection to any browser, hand a message to whichever API
  instance actually holds that connection, without either side needing to
  know which instance that is.
- **Why Pub/Sub is NOT a durable queue:** a Redis Pub/Sub channel with no
  subscribers simply drops the message — nothing is stored, nothing is
  redelivered later. This is a real, deliberate limitation, not an
  oversight.
- **What happens if an API/WebSocket instance is offline (or nobody has
  the bell open) when a delivery happens:** the in-app message is
  genuinely lost from the user's perspective — they just won't see it in
  the bell. It is still correctly recorded as `sent` in Postgres, because
  from the adapter's point of view, publishing to Redis succeeded; Redis
  simply had no subscribers. **This is why a future email/push adapter
  (not built yet) matters for "reach the user even when they're offline"**
  — that's explicitly out of scope for what Pub/Sub can ever provide.
- **Why Kafka remains the durable backbone, not Redis:** Kafka retains
  messages for a configured window regardless of consumers; Redis Pub/Sub
  retains nothing. If Redis were used as "the queue" instead of a fan-out
  mechanism sitting downstream of an already-durable Kafka message, an
  offline moment would mean permanent, silent data loss — not just a
  missed live notification.

---

## 11. WebSocket

**Why WebSocket** (over polling or SSE): a genuinely bidirectional,
persistent connection appropriate for a "live bell" — the server can push
the instant an event arrives, with no client-side polling delay and no
per-poll HTTP overhead.

**Connection lifecycle:** `GET /ws?user_id=...` upgrades the HTTP
connection (via `http.Hijacker`, taking it out of the normal request
lifecycle — this is why `http.Server.Shutdown` doesn't track it, see
section 2). The handler then runs until the connection closes for any
reason (client disconnect, ping timeout, or the API process exiting).

**Read pump:** a dedicated goroutine that does nothing but call
`ReadMessage()` in a loop. This project's WebSocket never expects an
application message *from* the client — the read pump exists purely to
process control frames (pong replies, close frames) and detect a dead
connection, cancelling a shared context on any read error.

**Write pump:** not a separate goroutine here — writes (pings and
real messages) happen from the same `select` loop that listens for the
Redis Pub/Sub channel and the ping ticker, since gorilla/websocket
requires all writes to a connection to be serialized from one goroutine
anyway.

**Ping/pong, deadlines, half-open connections:** detailed in section 2,
verified live (20s exact detection time).

**Redis Pub/Sub fan-out, horizontal scaling:**
```
worker (any instance)
  → Redis Pub/Sub channel "notify:inapp:<user_id>"
    → whichever api instance holds that user's WebSocket (subscribed to the same channel)
      → client
```
This is what lets `api` scale horizontally with zero code changes: any
number of instances can each run independently, and Redis Pub/Sub is what
lets a worker's delivery reach the *correct* one without either side
tracking where connections live globally.

---

## 12. Design decisions

| Decision | Why chosen | Alternative | Why not |
|---|---|---|---|
| Kafka (Redpanda locally) | Durable, ordered (per-key) log; partition-based horizontal scaling for consumers; the standard tool for this exact problem | RabbitMQ | Queues aren't naturally replayable/durable in the same way, and per-key ordering isn't a first-class concept the way Kafka partitioning provides it |
| PostgreSQL | Strong consistency and real transactions/constraints for the thing that actually needs correctness guarantees: delivery state | MongoDB | No unique-constraint-based atomic claim primitive as clean as Postgres's `INSERT ... ON CONFLICT`, and this data is inherently relational (rows referencing users/channels/events) |
| Redis Pub/Sub for WS fan-out | Lightweight, already-present infra (no new technology) for a "notify whoever's listening right now" problem that doesn't need durability | Publish directly from the worker to Kafka and have every API instance consume it | Every API instance would receive every event regardless of who's actually connected — far more wasted fan-out than one Redis channel per user |
| 3 Kafka partitions | Matches the "2-3 worker instances" scope; a real capacity-planning number | 1 partition (what auto-create silently gave us) | Caps the consumer group at exactly 1 useful worker no matter how many processes you run — found and fixed as a real bug, not a stylistic choice |
| Sequential worker (no pool) | Simple, no intra-process concurrency bugs possible; partitions already provide real parallelism via more worker processes | An internal goroutine pool per worker | Requires solving out-of-order-commit safety for no measured benefit — no load test yet shows a single sequential worker is the bottleneck |
| WebSocket | True push, bidirectional, appropriate for a live "bell" | SSE / polling | SSE is simpler but one-directional (fine here, we don't need client→server) and less universally what's expected for "real-time bell" demos; polling wastes requests and adds latency proportional to the poll interval |
| DLQ (Kafka topic + `dead` status) | Bounded retries with a visible, inspectable record of permanent failures | Infinite retry | Wastes resources forever on something that will never succeed, and can hammer an already-struggling downstream system indefinitely |

---

## 13. "Why did you do this?" questions

- **Why Kafka?** Durable, ordered, partition-scalable event backbone —
  the standard fit for "many producers, need replay/ordering/horizontal
  consumer scaling."
- **Why Redis if Kafka already exists?** Different job entirely — Kafka is
  the durable event log; Redis Pub/Sub is a lightweight "who's listening
  right now" fan-out for live WebSocket delivery. Using Kafka for that
  would mean every API instance consuming every event just to find the
  rare one it can actually deliver.
- **Why PostgreSQL?** It's the source of truth for delivery *state*, which
  needs real constraints (uniqueness), atomic conditional updates, and
  relational structure (events, channels, users, preferences) — not just
  a fast key-value cache.
- **Why 3 partitions?** Matches the planned worker count from the original
  scope; found the topic was silently auto-created with 1 (a real bug) and
  fixed it deliberately, not arbitrarily.
- **Why `user_id` as the Kafka key?** Guarantees one user's events are
  processed in the order they were published — the only ordering promise
  this system makes.
- **Why no worker pool?** Partitions already give real, correctly-ordered
  parallelism by running more worker *processes*; adding intra-process
  concurrency needs to solve out-of-order-commit safety, and nothing has
  shown that's actually needed yet.
- **Why not Redis as the queue?** Pub/Sub drops messages with no
  subscribers — using it as the durable queue would mean silent,
  permanent data loss the instant a consumer is briefly offline.
- **Why WebSocket?** True server push for a live notification bell, no
  polling delay.
- **Why not polling?** Wastes requests, and notification latency is
  bounded by the poll interval instead of being near-instant.
- **Why a DLQ?** Bounds retries and preserves failure information instead
  of looping forever or silently dropping permanently-broken deliveries.
- **Why exponential backoff?** Immediate, fixed-interval retries hit a
  struggling downstream system hardest exactly when it's least able to
  cope; growing the delay gives it room to recover.
- **Why jitter?** Prevents many simultaneously-failed deliveries from all
  retrying at the exact same instant and re-overwhelming whatever just
  came back.
- **Why idempotency?** Kafka is at-least-once by nature; without it, every
  redelivery (crash, rebalance, restart) would risk a duplicate
  notification.
- **Why not exactly-once?** No mechanism can make a Kafka offset commit, a
  Postgres write, and an external side effect (a Redis publish, an email
  send) atomic across three separate systems — the honest claim is
  idempotent *processing* under at-least-once delivery, not exactly-once
  delivery.
- **Why not put `Send()` inside a DB transaction?** It would hold row
  locks for the duration of an unpredictable external network call
  (blocking other workers) without actually achieving atomicity — the
  external system still isn't part of Postgres's transaction.
- **Why stale-claim recovery?** Without it, a worker crash between
  claiming a delivery and resolving it leaves that row stuck at `pending`
  forever — silently, permanently unretried. This was a real bug found in
  the Phase A audit, not a hypothetical.

---

## 14. Hard follow-up questions

- **"What if `Send()` succeeds but PostgreSQL crashes right after?"** The
  row stays `pending`; the reclaim scan retries it later, which means the
  external delivery can genuinely happen twice. This is the one honest
  gap in the design (section 4) — no amount of application code closes it
  without a distributed transaction spanning both systems, which is out
  of scope for this project's size.
- **"What if PostgreSQL succeeds but the Kafka commit fails?"** No
  problem — Kafka redelivers the message, and `ClaimNew` sees the row
  already exists and no-ops. Verified in
  `TestHandleNew_RedeliveredBeforeCommit_NotLostNotDuplicated`.
- **"Can the same notification be sent twice?"** For most crash windows,
  no — the unique constraint prevents a second claim from ever calling
  `Send()`. In the one specific window where a crash happens between a
  successful `Send()` and recording it, yes, it can — documented and
  accepted, not hidden.
- **"Can two workers process the same notification?"** Not concurrently —
  only one can win the `ClaimNew` insert; the loser does nothing. Verified
  by construction (Postgres unique constraint), same mechanism as above.
- **"What if a worker is slow but not dead?"** Its claim can be reclaimed
  once `StaleClaimTimeout` elapses, even though it's still "alive" — if it
  eventually finishes and tries to record a result, its fencing token
  (`claimed_at`) no longer matches, and the write is silently discarded.
  Verified in `TestStaleClaimRace_OldWorkerCannotOverwriteNewerState`.
- **"How do you know a pending claim is stale?"** Purely by elapsed time —
  `claimed_at` older than `StaleClaimTimeout`. There's no heartbeat or
  liveness check on the worker itself; a genuinely slow (not dead) worker
  and a genuinely dead one look identical after the timeout, which is
  exactly why the fencing-token protection in the previous answer matters.
- **"What if the stale worker comes back?"** Covered above — its write is
  discarded, not applied. It doesn't crash or error; `MarkSent` simply
  returns `applied=false`, which is logged.
- **"What if all 3 Kafka partitions are overloaded?"** Not tested — no
  load testing has been done yet (a later phase). The honest answer today:
  the sequential worker per partition would fall behind, consumer lag
  would grow, and delivery latency would increase; there's no
  backpressure or shedding mechanism implemented.
- **"What happens with 10 workers and 3 partitions?"** 3 get one partition
  each and do real work; 7 sit idle as standbys, joined to the group but
  unassigned, useful only if one of the active 3 dies and triggers a
  rebalance.
- **"What if Redis goes down?"** Verified live: in-app `Send()` calls fail
  with a dial error, classified as transient, scheduled for retry with
  backoff; once Redis returns, the reclaim scan picks them back up
  automatically. No crash, no lost events (beyond the one honest gap
  above).
- **"What if Kafka goes down?"** Verified live in Phase A/B: the worker's
  `FetchMessage` blocks/retries with kafka-go's own internal backoff (not
  code we wrote) — confirmed non-busy-looping (0% CPU) — and self-heals
  silently once the broker returns, with no visibility into "still
  retrying" beyond log lines (a real observability gap for a future
  metrics phase).
- **"How would you scale to 100K notifications/sec?"** Honestly: this
  hasn't been load-tested, so I don't have a measured number for where
  this design's ceiling actually is. The available levers, in order I'd
  reach for them: more Kafka partitions + matching worker processes (the
  primary lever, already proven to work); only then consider intra-process
  concurrency in the worker (the worker-pool tradeoff from section 2) if a
  load test showed a single sequential worker per partition was the
  bottleneck, not before.
- **"How would you guarantee ordering per user?"** Already done — keying
  by `user_id` puts one user's events on one partition, read by one
  consumer at a time. The one thing that *could* break this: a future
  intra-process worker pool processing multiple messages from the same
  partition concurrently, which is exactly why one wasn't added without
  solving that problem first.
- **"What is the current bottleneck?"** Unmeasured — no load testing has
  been run yet. Structurally, the most likely candidate is the sequential
  chain of blocking round trips inside one delivery attempt (Postgres
  claim → Postgres preference check → Redis publish → Postgres mark), all
  serialized per message within one worker.
- **"What would you measure before introducing a worker pool?"** Per-
  partition consumer lag under realistic load, and whether a single
  worker's sequential throughput (messages/sec) is actually below the
  partition's incoming rate — only then would the added complexity of
  concurrent, order-preserving processing be justified.

---

## 15. Things I must not claim

| Don't say | Why it's wrong | Say instead |
|---|---|---|
| "Exactly-once delivery" | No mechanism makes the Kafka offset, Postgres write, and external send atomic together | "At-least-once processing with idempotent application behavior" |
| "Zero duplicate notifications" | There's one specific crash window (Send succeeds, crash before MarkSent) where a duplicate external delivery is possible | "Duplicate *processing* is prevented by a unique constraint; duplicate *external delivery* is possible in one specific, narrow crash window, which is documented" |
| "Redis is the durable queue" | Pub/Sub drops messages with no subscribers — nothing is persisted | "Redis Pub/Sub is a fan-out mechanism for live delivery; Kafka is the durable backbone" |
| "Kafka guarantees exactly-once end-to-end" | Kafka's exactly-once semantics apply within Kafka-to-Kafka transactional pipelines, not to an arbitrary external side effect like a Redis publish | "Kafka guarantees at-least-once delivery to this consumer group; exactly-once to an external system isn't something Kafka alone can provide" |
| "A DB transaction makes Send() atomic" | Send() is never inside a DB transaction, and couldn't meaningfully be made atomic with one even if it were | "The DB transaction (a single atomic statement, in this design) protects the claim; the external send is a separate, non-atomic step" |
| "A worker pool provides unlimited concurrency" | Not built, and even if it were, correctness would require preserving per-partition commit ordering, which bounds how naively concurrent it could be | "Not implemented; partition count is the current parallelism lever, and a worker pool would need to solve out-of-order-commit safety first" |
| "This has been load tested" | It hasn't — no load testing phase has run yet | "Not implemented in the current version — a planned later phase" |
| "There's a replay mechanism for the DLQ" | Not built | "Dead-lettered events are inspectable via Postgres and the DLQ topic, but there's no replay endpoint yet — future improvement" |

---

## 16. Glossary

- **Kafka:** a distributed, partitioned, append-only log system used here
  as the durable event backbone between the API and the worker.
- **Topic:** a named log within Kafka; this project uses
  `notifications.events` and `notifications.events.dlq`.
- **Partition:** one ordered, independent slice of a topic; the unit of
  parallelism for a consumer group.
- **Offset:** a message's position within one partition; what a consumer
  commits to mark progress.
- **Producer:** anything that writes messages to a Kafka topic.
- **Consumer:** anything that reads messages from a Kafka topic.
- **Consumer Group:** a named set of consumers that split a topic's
  partitions between them, coordinated by the broker.
- **Rebalance:** the broker reassigning partitions when group membership
  changes.
- **Key:** an optional value on a Kafka message used to deterministically
  choose its partition via hashing.
- **At-least-once:** a delivery guarantee where a message may be processed
  more than once, but never zero times.
- **Idempotency:** the property that processing the same input more than
  once produces the same recorded result as processing it once.
- **DLQ (Dead Letter Queue):** where permanently-failed or retry-exhausted
  items go instead of being silently dropped or retried forever.
- **Retry:** attempting a failed operation again.
- **Backoff:** waiting between retries, typically for a growing amount of
  time (exponential backoff).
- **Jitter:** randomness added to a backoff delay to avoid many retries
  happening at the exact same instant.
- **Lease:** a time-bounded claim of ownership over a resource, after
  which it's considered abandoned and reclaimable.
- **Stale Claim:** a claim whose lease has expired without being resolved
  — usually because whoever held it crashed or hung.
- **Pub/Sub:** a messaging pattern where publishers send messages to a
  channel and any current subscribers receive them, with no persistence
  for those who weren't listening.
- **WebSocket:** a persistent, bidirectional connection protocol over TCP,
  used here for the live notification bell.
- **Ping/Pong:** an application-level heartbeat — one side sends a Ping
  control frame, the other replies with Pong, proving the connection is
  actually alive, not just TCP-connected.
- **Graceful Shutdown:** handling a termination signal by finishing
  current work and cleaning up resources, instead of being killed
  mid-operation.
- **ACID:** Atomicity, Consistency, Isolation, Durability — the guarantees
  a database transaction provides; relevant here because Postgres provides
  these *within* a single statement/transaction, but nothing extends them
  across Postgres, Kafka, and an external side effect together.
