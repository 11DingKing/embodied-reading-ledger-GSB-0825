# Embodied Reading Event Ledger

A Go backend that faithfully records the **real, embodied process** of reading a
physical book — not a summary of its content. A reader registers a book edition,
opens a session, and appends an ordered stream of events:

`SESSION_STARTED → PAGE_REACHED → PASSAGE_REACTED → INTERRUPTED → SESSION_ENDED`

From this append-only ledger the server reconstructs, with deterministic
server-side logic:

- **reading minutes** (time actually spent reading, excluding interruption gaps)
- **last page reached**
- **interruption count**
- the reader's **own hand-written reactions** to passages

The machine does not summarize anything. It only preserves what actually
happened, in order, in UTC, with no UPDATE/DELETE possible on the event stream.

## Tech stack

- Go 1.23 (`net/http` ServeMux routing — no third-party router)
- PostgreSQL 16
- [`pgx/v5`](https://github.com/jackc/pgx) (pgxpool + native transactions)
- No Redis, no in-process mutexes — **all correctness lives in PostgreSQL**
  (`SELECT … FOR UPDATE`, transaction-scoped advisory locks, unique constraints)

## Architecture

```
┌─────────────┐   HTTP/JSON   ┌──────────────┐   pgx    ┌───────────────┐
│   client    │ ────────────▶ │  httpapi     │ ───────▶ │  PostgreSQL   │
│ (curl/app)  │ ◀──────────── │  (net/http)  │ ◀─────── │  16           │
└─────────────┘               └──────┬───────┘          └───────────────┘
                                    │
                              ┌──────▼───────┐
                              │   service    │  state machine · metrics
                              │              │  idempotency · validation
                              └──────┬───────┘
                                     │
                              ┌──────▼───────┐
                              │    store     │  pgx data access
                              └──────────────┘
```

Layering (all under `internal/`):

| Package      | Responsibility |
|--------------|----------------|
| `config`     | Environment configuration (`PORT`, `DATABASE_URL`, `DB_MAX_CONNS`) |
| `database`   | Connection pool, embedded SQL migration runner |
| `domain`     | Core models, the `Timestamp` (UTC RFC3339Nano) type, event/session enums |
| `store`      | All SQL against books/sessions/events/idempotency_keys |
| `service`    | State machine, monotonic-time validation, metrics, transactions, idempotency orchestration |
| `httpapi`    | Routing, request/response codecs, structured errors, logging & recovery |
| `seed`       | Deterministic, re-runnable seed fixture |

Entry points:

- `cmd/server` — the API server (auto-migrates on startup)
- `cmd/seed` — applies the deterministic seed fixture

## Data model

- **books** — a physical edition (ISBN, title, author, publisher, `total_pages`, format).
- **sessions** — one continuous reading attempt for a book; status moves
  `pending → active → ended`.
- **events** — the append-only ledger. Columns: `(session_id, seq, event_type,
  occurred_at, page, note, quote, reason, recorded_at)`. A trigger
  `events_append_only` rejects **every** `UPDATE`/`DELETE` with `check_violation`.
  `UNIQUE(session_id, seq)` is a hard guarantee.
- **idempotency_keys** — stores the exact first response (status + raw body) for
  each `Idempotency-Key`, so retries never double-write.

## State machine & validation rules

A session starts in `pending` with `max(seq) = 0`.

| Current state | Event            | New state |
|---------------|------------------|-----------|
| `pending`     | `SESSION_STARTED` | `active`  |
| `active`      | `PAGE_REACHED`    | `active`  |
| `active`      | `PASSAGE_REACTED`| `active`  |
| `active`      | `INTERRUPTED`     | `active`  |
| `active`      | `SESSION_ENDED`   | `ended`   |
| `ended`       | *(any)*           | rejected  |

Enforced by the server (inside a transaction):

1. **First event must be `SESSION_STARTED`** — a page reached before start is
   rejected with `SESSION_NOT_STARTED` (422).
2. **`SESSION_STARTED` may only occur once** — a second one is rejected with
   `SESSION_ALREADY_STARTED` (422).
3. **No events after `SESSION_ENDED`** — `SESSION_ALREADY_ENDED` (422).
4. **Monotonic client time** — each `occurredAt` must be ≥ the previous event's
   `occurredAt`, otherwise `TIMESTAMP_NOT_MONOTONIC` (422).
5. **Page bounds** — `PAGE_REACHED` requires `page > 0` and `page ≤ total_pages`
   when the book has a known page count (`INVALID_PAGE`, 422).
6. **Reactions require a note** — `PASSAGE_REACTED` without `note` is rejected
   (`NOTE_REQUIRED`, 422).

### Reading-minutes calculation

Durations are computed server-side from adjacent event pairs:

```
readingDuration = Σ (events[i].occurredAt − events[i−1].occurredAt)
                  for each i where events[i−1].type != INTERRUPTED
```

The gap following an `INTERRUPTED` event (the interruption itself, until the next
event) is **not** counted. An active session only sums completed intervals; it
never invents time up to "now". Returned as both `readingDurationSeconds`
(integer) and `readingMinutes` (float).

### Concurrency control

Every event append carries `expectedSeq` — the sequence the client believes it
is appending after (`0` before the first event). Inside one transaction:

1. `SELECT … FROM sessions WHERE id = $1 FOR UPDATE` — serializes all writers to
   the session.
2. `SELECT COALESCE(MAX(seq),0) FROM events WHERE session_id = $1` — the true
   current sequence.
3. If `expectedSeq != currentSeq` → **409 `SEQ_CONFLICT`** with
   `{ "currentSeq": N, "expectedSeq": M }`. No insert happens.
4. Otherwise insert with `seq = currentSeq + 1` and advance status.

Result: of N concurrent appends at the same `expectedSeq`, **exactly one**
succeeds; the others receive 409 and the current sequence. `UNIQUE(session_id,
seq)` is a backstop.

### Idempotency

All `POST` endpoints accept an `Idempotency-Key` header. The body is hashed
(SHA-256). The server takes a transaction-scoped **PostgreSQL advisory lock** on
the key (`pg_advisory_xact_lock(hashtextextended($1,0))`) — no Redis, no
in-memory lock — and:

- If the key is unknown: execute the write, then store `(status, response_body,
  request_hash, method, path)` in `idempotency_keys` and commit atomically.
- If the key exists with the **same** method/path/hash: replay the stored status
  and exact body bytes — no second write.
- If the key exists but with a **different** body: **422
  `IDEMPOTENCY_KEY_REUSED`**.

Requests without an `Idempotency-Key` execute normally (and are not de-duplicated).

## HTTP API

Base URL: `http://localhost:8080`. Full spec in [`openapi/openapi.yaml`](openapi/openapi.yaml).

| Method | Path                       | Purpose |
|--------|----------------------------|---------|
| POST   | `/books`                   | Register a book edition |
| POST   | `/sessions`                | Create a reading session |
| POST   | `/sessions/{id}/events`    | Append an event (requires `expectedSeq`) |
| GET    | `/sessions/{id}`           | Session + computed metrics + full event log |
| GET    | `/healthz`                 | Health check |

All non-2xx responses share a structured body:

```json
{
  "error": {
    "code": "SEQ_CONFLICT",
    "message": "expectedSeq does not match the current sequence",
    "details": { "currentSeq": 2, "expectedSeq": 0 }
  }
}
```

### Example: append an event

```bash
curl -s -X POST http://localhost:8080/sessions/$SID/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: ev-42' \
  -d '{
    "eventType": "PAGE_REACHED",
    "occurredAt": "2026-08-25T10:10:00.123456789Z",
    "page": 42,
    "expectedSeq": 1
  }'
```

All timestamps are accepted as RFC3339/RFC3339Nano (with any offset), normalized
to **UTC**, and emitted as **RFC3339Nano** in every response.

## Quick start

### 1. Start PostgreSQL

```bash
docker compose up -d db
```

This starts Postgres 16 on `localhost:5432` (credentials `ledger/ledger`,
database `ledger`). If port 5432 is taken on your machine, override it:

```bash
DB_PORT=5436 docker compose up -d db
export DATABASE_URL="postgres://ledger:ledger@localhost:5436/ledger?sslmode=disable"
```

### 2. Download dependencies, build, test

```bash
go mod download
go build ./...
go test ./...
```

### 3. (Optional) Load the deterministic seed fixture

```bash
go run ./cmd/seed
```

Seeds a fixed book and a 6-event session whose metrics are deterministic:
**35 reading minutes, last page 40, 1 interruption, 1 passage reaction**.

### 4. Run the server

```bash
go run ./cmd/server
```

The server listens on `:8080` (override with `PORT`) and applies pending
migrations on startup.

## Acceptance / verification commands

From a fresh clone, with Docker running:

```bash
# 1. Database
docker compose up -d db

# 2. Dependencies & build
go mod download
go build ./...

# 3. Tests (integration — connects to Postgres)
go test ./...

# 4. Race-detector run for the concurrency suite
go test ./... -race

# 5. Run the server
go run ./cmd/server
```

The integration tests in [`test/integration/`](test/integration/) cover:

- the full happy path and exact metrics computation (including the interruption
  gap exclusion),
- illegal state transitions (first event not `SESSION_STARTED`, double start,
  event after end, non-monotonic time, out-of-range page, missing note, unknown
  event type),
- `expectedSeq` mismatch returning 409 with `currentSeq`,
- **concurrent appends** (25 goroutines at the same `expectedSeq` → exactly one
  201, twenty-four 409s, only one row persisted),
- **idempotency replay** (byte-identical first response, no duplicate rows) and
  key reuse with a different body (422),
- the append-only trigger rejecting direct `UPDATE`/`DELETE`,
- timezone offsets normalized to UTC,
- the deterministic seed fixture.

Tests honor `TEST_DATABASE_URL` (then `DATABASE_URL`), defaulting to
`postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable`.

## Configuration

| Env var          | Default                                                        |
|------------------|----------------------------------------------------------------|
| `PORT`           | `8080`                                                         |
| `DATABASE_URL`   | `postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable` |
| `DB_MAX_CONNS`   | `10`                                                           |
| `DB_PORT`        | `5432` (compose host-port bind only)                           |

## Project layout

```
.
├── cmd/
│   ├── server/main.go        # API entry point
│   └── seed/main.go          # seed loader
├── internal/
│   ├── config/               # env config
│   ├── database/             # pool + embedded migration runner
│   │   └── migrations/       # 0001_init.up.sql / .down.sql
│   ├── domain/               # models, Timestamp, enums
│   ├── httpapi/              # handlers, routing, middleware, errors
│   ├── seed/                 # deterministic seed.sql
│   ├── service/              # state machine, transactions, idempotency, metrics
│   └── store/                # pgx data access
├── openapi/openapi.yaml      # API specification
├── test/integration/         # end-to-end integration tests
├── docker-compose.yml
├── go.mod / go.sum
└── README.md
```
