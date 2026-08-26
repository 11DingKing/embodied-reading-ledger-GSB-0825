# Embodied Reading Event Ledger

A backend for the *lived* process of reading a physical book. You register a
book edition, open a reading session, and append the events that actually
happened — `SESSION_STARTED`, `PAGE_REACHED`, `PASSAGE_REACTED`, `INTERRUPTED`,
`SESSION_ENDED`. The server never summarizes on your behalf. It only faithfully
stores what you recorded and *reconstructs* the facts from the raw stream:
reading minutes, last page reached, how many times you were interrupted, and the
reactions you wrote in your own words.

The ledger is **append-only** and the correctness guarantees (idempotency,
per-session sequencing, single-winner concurrency) live in **PostgreSQL** —
there are no Redis, no in-memory locks, and no application-level caches deciding
who wins a race.

---

## Architecture

```
cmd/server        HTTP entrypoint: connect → migrate → serve (graceful shutdown)
cmd/seed          Loads the deterministic fixture

internal/domain   Pure core: event types, state machine, projection (no I/O)
internal/store    PostgreSQL persistence: all SQL + transactional guarantees
internal/httpapi  net/http handlers, structured JSON errors, idempotency wiring
internal/apperr   Stable, structured error vocabulary (Code + message + details)
internal/clock    UTC RFC3339Nano everywhere; injectable clock for tests
internal/config   Env-driven config with docker-compose defaults
internal/seed     Deterministic fixture (fixed UUIDs + fixed instants)

migrations/        Embedded SQL migrations (go:embed), applied at startup
api/openapi.yaml   OpenAPI 3.1 description of every endpoint and error body
test/integration   End-to-end tests against a real Postgres (schema-isolated)
```

### Layering

The **domain** layer is pure and I/O-free: given prior state and a parsed event,
it decides whether the transition is legal and folds it into new state, and it
projects an ordered event slice into the reading story. The **store** layer owns
every SQL statement and every transactional guarantee. The **httpapi** layer is
deliberately thin: parse, validate, delegate to the store's idempotent executor,
render JSON.

### Where correctness actually lives (PostgreSQL, not Go)

- **Per-session ordering / single-winner concurrency** — `events` has a
  `UNIQUE (session_id, seq)` constraint. Two requests racing to append at the
  same `seq` both try to `INSERT`; exactly one commits, the other hits the unique
  violation and is translated into `409 SEQ_CONFLICT` carrying the authoritative
  `currentSeq`. No mutex, no advisory lock — the constraint is the arbiter.
- **Idempotency** — a first-class `idempotency_keys` table. A write claims its
  key via `INSERT ... ON CONFLICT DO UPDATE` inside the same transaction as the
  business write, so the event and the stored response commit atomically.
  Concurrent same-key requests serialize on that row lock; the second observes
  the first's committed response and **replays it verbatim** without a second
  landing. Reusing a key with a different body is rejected with
  `409 IDEMPOTENCY_KEY_REUSE`.
- **Append-only** — a `BEFORE UPDATE OR DELETE` trigger on `events` raises an
  exception, so the ledger physically cannot be rewritten, even from `psql`.
- **Time** — all instants are stored as canonical UTC `RFC3339Nano` **text** so
  nanosecond precision round-trips exactly (Postgres `timestamptz` only keeps
  microseconds). Durations are computed server-side from adjacent events; clients
  never assert elapsed minutes.

---

## State machine

Every session's events form a strict sequence, validated on append:

```
              ┌─────────────────────────────────────────────┐
   (empty) ── SESSION_STARTED ──► started ──► ... ──► SESSION_ENDED ──► sealed
                                    │  ▲                                   │
              PAGE_REACHED ─────────┤  │                                   │
              PASSAGE_REACTED ──────┤  │  (any number, in any order,       │
              INTERRUPTED ──────────┘  │   while started and not ended)    │
                                       └───────────────────────────────────┘
```

Rules enforced by [internal/domain/event.go](internal/domain/event.go), each
mapping to a **stable error code**:

| Situation | Code | HTTP |
|---|---|---|
| First event isn't `SESSION_STARTED`, or a second `SESSION_STARTED` | `INVALID_TRANSITION` | 422 |
| `occurredAt` earlier than the previous event | `TIME_REGRESSION` | 422 |
| `PAGE_REACHED` before the session start instant | `PAGE_BEFORE_START` | 422 |
| Any event after `SESSION_ENDED` | `EVENT_AFTER_END` | 422 |
| `expectedSeq` ≠ current+1, or a lost concurrent race | `SEQ_CONFLICT` (with `currentSeq`) | 409 |
| Idempotency-Key reused with a different body | `IDEMPOTENCY_KEY_REUSE` | 409 |
| Missing/invalid input | `VALIDATION` | 400 |
| Unknown book/session | `NOT_FOUND` | 404 |

### How the reading story is reconstructed

- **readingMinutes** = span from `SESSION_STARTED` to the last event, **minus**
  every interval spent interrupted. An `INTERRUPTED` opens a gap; the next event
  closes it. This is time actually spent *with the book*, not raw wall-clock.
- **lastPage** = the page of the most recent `PAGE_REACHED`.
- **interruptionCount** = number of `INTERRUPTED` events.
- **feelings** = every `PASSAGE_REACTED`'s reader-written words, in ledger order.

---

## API

Base URL defaults to `http://localhost:8080`. Full contract in
[api/openapi.yaml](api/openapi.yaml). All write endpoints accept an optional
`Idempotency-Key` header.

| Method & path | Purpose |
|---|---|
| `POST /books` | Register a physical book edition |
| `POST /sessions` | Open a reading session for a book |
| `POST /sessions/{id}/events` | Append one event (`expectedSeq` required) |
| `GET /sessions/{id}` | Reconstruct the session's reading story |
| `GET /healthz` | Liveness |

Every error uses one envelope:

```json
{ "error": { "code": "SEQ_CONFLICT", "message": "...", "details": { "currentSeq": 1 } } }
```

---

## Running it

### Prerequisites
- Go 1.23+ (developed and verified on the Go 1.26 toolchain)
- Docker + Docker Compose

### Native paths

```bash
docker compose up -d db     # PostgreSQL 16, published on host port 5439
go mod download             # fetch dependencies
go test ./...               # unit + integration tests (needs the db up)
go build ./...              # compile everything
go run ./cmd/server         # migrate on boot, then serve on :8080
```

> **Port note:** the compose file maps Postgres to host port **5439** to avoid
> clashing with other local Postgres instances. The app's default
> `DATABASE_URL` matches. Override with the `DATABASE_URL` env var if needed;
> override the HTTP port with `ADDR` (e.g. `ADDR=:8099`).

### Optional: load the deterministic fixture

```bash
go run ./cmd/seed
curl -s localhost:8080/sessions/00000000-0000-0000-0000-0000000000c1 | jq .projection
```

### Try it by hand

```bash
BID=$(curl -s -X POST localhost:8080/books -H 'Content-Type: application/json' \
  -d '{"isbn":"9780374528379","title":"The Brothers Karamazov"}' | jq -r .id)

SID=$(curl -s -X POST localhost:8080/sessions -H 'Content-Type: application/json' \
  -d "{\"bookId\":\"$BID\",\"reader\":\"Ada\"}" | jq -r .id)

curl -s -X POST localhost:8080/sessions/$SID/events -H 'Content-Type: application/json' \
  -d '{"expectedSeq":1,"type":"SESSION_STARTED","occurredAt":"2026-08-26T09:00:00Z"}'

curl -s localhost:8080/sessions/$SID | jq .projection
```

---

## Acceptance commands

```bash
docker compose up -d db     # start PostgreSQL 16
go mod download             # install dependencies
go test ./...               # all tests green (integration hits real Postgres)
go build ./...              # builds with no errors
go run ./cmd/server         # boots, applies migrations, serves
```

The integration suite in [test/integration](test/integration) covers, against a
**real** database, each in its own isolated schema:

- **Concurrency race** — 12 goroutines append at `seq 1` simultaneously; exactly
  one wins (201), the other 11 get `409 SEQ_CONFLICT`, and the ledger holds one
  event.
- **Idempotent replay** — the same key + body returns the first response
  byte-for-byte and lands only one row; a different body reuse returns 409.
- **Illegal transitions** — first-event, time-regression, and event-after-end
  each return their stable code.

Unit tests in [internal/domain](internal/domain) prove the state machine and the
minutes/last-page/interruption/feelings projection in isolation.

---

## Design decisions worth calling out

- **Text timestamps, not `timestamptz`** — to preserve nanosecond precision
  exactly on the round trip, as the spec requires.
- **`seq` is client-asserted via `expectedSeq`** — this makes every append an
  optimistic-concurrency operation with an explicit contract, and turns the
  race into a clean, observable 409 instead of a silent lost write.
- **Idempotency response stored as bytes** — replay is literally the original
  response, so a retried request can never observe a divergent view.
- **Tests skip (don't fail) when no DB is reachable** — so `go test ./...` is
  honest on a machine without the container, while the acceptance path expects
  the db up and exercises the real guarantees.
