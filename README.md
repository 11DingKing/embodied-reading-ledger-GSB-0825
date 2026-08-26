# Embodied Reading Ledger（具身阅读事件账本）

一个只追加（append-only）的阅读过程账本后端：读者登记实体书版本、创建阅读会话，
再按顺序记录真实发生的事件——`SESSION_STARTED`、`PAGE_REACHED`、
`PASSAGE_REACTED`、`INTERRUPTED`、`SESSION_ENDED`。系统不做任何机器总结，
只从事件流中**还原**：阅读分钟数、最后页码、打断次数，以及读者亲手写下的感受。

## 技术栈与硬约束

- Go 1.23（`go.mod` 声明 `go 1.23`）、仅标准库 `net/http`（Go 1.22+ 模式路由）
- PostgreSQL 16 + `pgx/v5`
- **无 Redis、无内存锁**：并发正确性完全由 PostgreSQL 事务与唯一约束保证
- 事件表只能追加：`UPDATE`/`DELETE` 被数据库触发器拒绝
- 所有时间存 UTC，序列化为 RFC3339Nano；持续时间由服务端按相邻事件计算

## 架构

```
cmd/server            HTTP 入口（环境变量、迁移、优雅停机）
internal/api          net/http 处理器、JSON、Idempotency-Key 契约、结构化错误
internal/store        pgx 连接池、迁移运行器（go:embed）、事务化业务逻辑
internal/store/migrations   嵌入式 SQL 迁移（启动时按文件名顺序应用）
internal/errs         稳定错误码的结构化错误类型
seed/seed.sql         确定性种子夹具（固定 UUID 与时间戳）
openapi.yaml          OpenAPI 3.0 规范
docker-compose.yml    PostgreSQL 16（宿主机端口 5433 → 容器 5432）
```

### 正确性如何落在 PostgreSQL

1. **并发写入（CAS）**：追加事件时执行
   `UPDATE reading_sessions SET last_seq = last_seq + 1 WHERE id = $1 AND last_seq = $2`。
   同一会话的并发写者中只有一个能更新成功（行锁串行化），其余得到
   `409 E_SEQ_CONFLICT` 及 `details.current_seq`。`reading_events` 上的
   `UNIQUE(session_id, seq)` 是数据库兜底。
2. **幂等**：每个写请求必须携带 `Idempotency-Key`。同一事务内先
   `INSERT ... ON CONFLICT DO NOTHING` 抢占键；抢到则执行业务逻辑，并把
   状态码 + 响应体与业务写入**同一事务**提交。未抢到则取出首次响应**逐字节**
   重放（`response_body` 用 `text` 而非 `jsonb`，避免重排），绝不二次落库。
   同一键配不同请求体返回 `422 E_IDEMPOTENCY_MISMATCH`。失败请求回滚，
   不会消耗幂等键。并发同键请求会在唯一索引上阻塞至首个事务提交，随后走重放。
3. **只追加**：`reading_events` 上的 `BEFORE UPDATE OR DELETE` 触发器直接抛错，
   任何路径（含直连 SQL）都无法改写历史。

### 状态机

```
                (会话创建, last_seq = 0)
                        │  SESSION_STARTED (必须为首事件)
                        ▼
      ┌────────────  OPEN  ◄────────────────┐
      │                 │                   │
  PAGE_REACHED   PASSAGE_REACTED      INTERRUPTED
      │                 │                   │
      └────────────► 流转中 ── SESSION_ENDED ──► ENDED（终态，禁止再追加）
```

稳定错误码（结构化错误体 `{"error":{"code","message","details?"}}`）：

| 状态 | code | 含义 |
| --- | --- | --- |
| 400 | `E_VALIDATION` | 请求体/参数非法（页码越界、缺 reaction 等） |
| 400 | `E_IDEMPOTENCY_REQUIRED` | 写请求缺少 `Idempotency-Key` |
| 404 | `E_NOT_FOUND` | 资源不存在 |
| 409 | `E_SEQ_CONFLICT` | `expected_seq` 与当前序号不一致，`details.current_seq` 给出当前值 |
| 409 | `E_APPEND_AFTER_END` | 会话已结束仍追加事件 |
| 409 | `E_IDEMPOTENCY_IN_PROGRESS` | 同键首次请求仍在进行中 |
| 422 | `E_IDEMPOTENCY_MISMATCH` | 同一幂等键配不同请求体 |
| 422 | `E_PAGE_BEFORE_START` | 会话开始前到达页码 |
| 422 | `E_TIME_REGRESSION` | 客户端时间倒退（早于上一事件） |
| 422 | `E_INVALID_STATE_TRANSITION` | 非法状态迁移（如重复 `SESSION_STARTED`） |

### 时间语义

- `occurred_at`：客户端上报，必须为 RFC3339Nano；校验后统一转 UTC 存储。
- `recorded_at`：服务端接收时间（UTC），由数据库 `now()` 生成。
- 持续时间只由服务端按相邻事件的 `occurred_at` 差值计算
  （`duration_since_previous_seconds`，首事件为 0），客户端不得上报时长。
- `reading_minutes` = 相邻事件差值之和 ÷ 60（ telescoping 后等价于末事件 − 首事件）。

## 启动

```bash
docker compose up -d db      # 启动 PostgreSQL 16（宿主机端口 5433）
go mod download              # 下载依赖
go build ./...               # 构建
go run ./cmd/server          # 自动应用迁移，监听 :8080
```

环境变量：`DATABASE_URL`（默认
`postgres://postgres:postgres@localhost:5433/reading_ledger?sslmode=disable`）、
`ADDR`（默认 `:8080`）。

## 验收

```bash
go test ./...                # 集成测试（自动建 reading_ledger_test 库；需先起 db）
```

测试覆盖：并发竞争（16 并发写同一 `expected_seq`，恰 1 个 201、其余 409 +
`current_seq`）、幂等重放（响应逐字节一致且不二次落库、异体同键 422）、
非法状态迁移（开始前到页、重复开始、结束后追加）、客户端时间倒退、
只追加触发器（直改 SQL 被拒）、完整会话还原（分钟数/最后页码/打断次数/感受）。
测试库连接可用 `TEST_DATABASE_URL` / `ADMIN_DATABASE_URL` 覆盖。

种子夹具（确定性，固定 UUID/时间）：

```bash
docker compose exec -T db psql -U postgres -d reading_ledger < seed/seed.sql
curl -s http://localhost:8080/sessions/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
```

## 接口一览

- `POST /books` — 登记实体书版本（title/author/edition/isbn/total_pages）
- `POST /sessions` — 创建会话（book_id/reader_name）
- `POST /sessions/{id}/events` — 追加事件（必须携带 `Idempotency-Key` 与 `expected_seq`）
- `GET /sessions/{id}` — 还原会话（事件流、服务端计算的时长、分钟数、最后页码、打断次数、感受）

详见 [openapi.yaml](openapi.yaml)。

## 调用示例

```bash
curl -X POST http://localhost:8080/books \
  -H 'Idempotency-Key: b1' \
  -d '{"title":"The Peregrine","author":"J. A. Baker","edition":"NYRB 2005","isbn":"978-1590171332","total_pages":192}'

curl -X POST http://localhost:8080/sessions \
  -H 'Idempotency-Key: s1' \
  -d '{"book_id":"<book_id>","reader_name":"Ada"}'

curl -X POST http://localhost:8080/sessions/<session_id>/events \
  -H 'Idempotency-Key: e1' \
  -d '{"type":"SESSION_STARTED","occurred_at":"2026-08-01T20:00:00Z","expected_seq":0}'
```
