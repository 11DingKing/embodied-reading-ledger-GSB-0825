# 具身阅读事件账本（Embodied Reading Event Ledger）

一个忠实记录**真实阅读过程**的后端：登记实体书版本 → 创建会话 → 按发生顺序追加
`SESSION_STARTED`、`PAGE_REACHED`、`PASSAGE_REACTED`、`INTERRUPTED`、`SESSION_ENDED`
事件。系统不做任何内容总结，只负责把事件可靠地存下来，并据此还原：

- **阅读分钟数**（服务端按相邻事件时间差计算，中断时段不计入）
- **最后页码 / 最深页码**
- **打断次数**
- **读者亲手写下的感受**（`PASSAGE_REACTED.note`，原文返回，绝不二次加工）

## 技术栈

- Go 1.23（仅用标准库 `net/http` 做路由，Go 1.22+ 方法路由）
- PostgreSQL 16
- `github.com/jackc/pgx/v5`（连接池 + 事务）
- 无 Redis、无内存锁、无 ORM；并发正确性全部由 PostgreSQL 事务与约束保证

## 架构

```
cmd/server          HTTP 入口：启动时自动执行迁移，监听 :8080
cmd/seed            灌入确定性种子夹具（幂等，可重复执行）
internal/config     PORT / DATABASE_URL 环境变量
internal/database   pgxpool 连接、go:embed 迁移执行器、种子 SQL
internal/ledger     纯领域逻辑：事件类型、状态机校验、阅读汇总（无 I/O 依赖）
internal/api        net/http 处理器、幂等键、结构化错误体
test/               面向真实 PostgreSQL 的集成测试
```

请求路径上的关键事务（以追加事件为例，全部在**一个数据库事务**内完成）：

1. `INSERT INTO idempotency_keys ... ON CONFLICT (key) DO NOTHING RETURNING key`
   认领幂等键；已存在则校验请求指纹、返回首次响应或 409。
2. `SELECT ... FROM reading_sessions ... FOR UPDATE OF rs` 锁定会话行——
   同一会话的并发追加在此串行化，**不使用任何进程内锁**。
3. 读取当前最大序号事件，校验 `expectedSeq` 与状态机规则。
4. `INSERT INTO events ...`（事件表只允许 INSERT）。
5. 提交后返回新序号；失败回滚（幂等键占用一并回滚，可安全重试）。

## 数据模型

| 表 | 说明 |
| --- | --- |
| `books` | 实体书版本（ISBN、书名、作者、版次、总页数） |
| `reading_sessions` | 阅读会话，`status ∈ {open, ended}` |
| `events` | 仅追加事件流，主键 `(session_id, seq)`；带触发器禁止 UPDATE/DELETE |
| `idempotency_keys` | 幂等键 → 请求指纹 + 首次响应（TEXT 原样保存，重放逐字节一致） |

`events.event_type` 由 CHECK 约束限定为五种合法类型；`events` 表上装有
`BEFORE UPDATE OR DELETE` 触发器，任何修改尝试都会被数据库直接拒绝。

## 会话状态机

```
                 SESSION_STARTED (seq=1)
  (无事件) ───────────────────────────────▶▶ 阅读中 (open)
                                              │
       PAGE_REACHED / PASSAGE_REACTED ◀───────┤  （可重复、可交错）
      INTERRUPTED                             │
                                              │  SESSION_ENDED
                                              ▼
                                          已结束 (ended)
                                     此后追加任何事件 → 422
```

校验规则（违反时返回稳定错误码）：

| 规则 | 错误码 | HTTP |
| --- | --- | --- |
| 首个事件必须是 `SESSION_STARTED`（开始前到达页码） | `SESSION_NOT_STARTED` | 422 |
| `SESSION_STARTED` 只能出现一次 | `SESSION_ALREADY_STARTED` | 422 |
| `SESSION_ENDED` 后禁止追加（结束后追加事件） | `SESSION_ALREADY_ENDED` | 422 |
| `occurredAt` 必须严格晚于上一事件（时间倒退/相等） | `CLOCK_WENT_BACKWARDS` | 422 |
| 页码超出 `1..totalPages` | `PAGE_OUT_OF_RANGE` | 422 |
| `PASSAGE_REACTED.note` 必填、payload 拒绝未知字段 | `INVALID_EVENT_PAYLOAD` | 422 |
| `expectedSeq` ≠ 服务端当前序号 | `SEQUENCE_CONFLICT`（details 带 `currentSeq`） | 409 |
| 幂等键被不同请求体重用 | `IDEMPOTENCY_KEY_REUSED` | 409 |
| 同幂等键请求仍在进行中 | `IDEMPOTENCY_IN_FLIGHT` | 409 |
| JSON/字段/UUID/时间戳格式错误 | `VALIDATION_ERROR` / `INVALID_TIMESTAMP` | 400 |
| 资源不存在 | `BOOK_NOT_FOUND` / `SESSION_NOT_FOUND` | 404 |

错误响应统一为结构化错误体：

```json
{
  "error": {
    "code": "SEQUENCE_CONFLICT",
    "message": "expected seq 1 but session is at seq 2",
    "details": { "expectedSeq": 1, "currentSeq": 2 }
  }
}
```

## 并发与幂等语义

- **乐观序号 `expectedSeq`**：追加事件时客户端必须携带自己认知的当前最大序号
  （首个事件为 `0`）。服务端在会话行锁内比对，不一致即返回 `409` 与当前序号。
  同一会话并发写入只有一个成功，其余全部冲突。
- **`Idempotency-Key` 头**：所有写接口（`POST /books`、`POST /sessions`、
  `POST /sessions/{id}/events`）均接受。携带相同键 + 相同请求体重放时，服务端
  返回**首次响应原文**（响应头带 `Idempotent-Replay: true`）且不二次落库；
  相同键 + 不同请求体返回 `409 IDEMPOTENCY_KEY_REUSED`。
- 失败的请求（4xx/5xx）随事务回滚，不占用幂等键，修正后可用同键重试。

## 时间与阅读时长

- 所有时间以 **UTC RFC3339Nano** 传输与存储（PostgreSQL `timestamptz`，微秒精度）。
- 阅读时长由服务端按**相邻事件**计算：对每一对相邻事件 `(e[i], e[i+1])` 累加
  `occurredAt[i+1] - occurredAt[i]`；若 `e[i]` 是 `INTERRUPTED`，该段间隔视为
  中断、不计入阅读分钟数。`readingMinutes` 即累计时长的分钟数。

## 快速开始

```bash
# 1. 启动 PostgreSQL 16（主机端口 5433 -> 容器 5432，避让本机已占用的 5432）
docker compose up -d db

# 2. 下载依赖
go mod download

# 3. 运行测试（自动执行迁移；连不上库会提示先执行第 1 步）
go test ./...

# 4. 构建
go build ./...

# 5. 启动服务（启动时自动迁移；默认监听 :8080）
go run ./cmd/server

# 可选：灌入确定性种子夹具（幂等，可重复执行）
go run ./cmd/seed
```

可通过环境变量覆盖：`DATABASE_URL`（默认
`postgres://ledger:ledger@localhost:5433/ledger?sslmode=disable`）、`PORT`（默认 `8080`）。
若你的 5432 端口空闲且希望使用标准端口，可把 `docker-compose.yml` 的端口映射改回
`"5432:5432"` 并相应调整 `DATABASE_URL`。

## 接口一览

| 方法与路径 | 说明 |
| --- | --- |
| `POST /books` | 登记实体书版本 |
| `POST /sessions` | 创建阅读会话 |
| `POST /sessions/{id}/events` | 追加事件（需 `expectedSeq`，接受 `Idempotency-Key`） |
| `GET /sessions/{id}` | 读取事件流与还原结果（阅读分钟数、最后页码、打断次数、感受） |
| `GET /healthz` | 健康检查 |

完整契约见 [openapi.yaml](openapi.yaml)。

### curl 示例

```bash
# 登记一本书
curl -sS -X POST localhost:8080/books \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: book-1' \
  -d '{"isbn":"978-7-111-00000-1","title":"纸上的钟","author":"林晚","edition":"2024年第1版","totalPages":320}'

# 创建会话（用上一步返回的 id）
curl -sS -X POST localhost:8080/sessions \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: sess-1' \
  -d '{"bookId":"<book-id>","readerTag":"alice"}'

# 开始阅读（expectedSeq=0）
curl -sS -X POST localhost:8080/sessions/<session-id>/events \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: ev-1' \
  -d '{"expectedSeq":0,"event":{"type":"SESSION_STARTED","occurredAt":"2026-08-25T10:00:00Z","payload":{}}}'

# 读到第 42 页（expectedSeq=1）
curl -sS -X POST localhost:8080/sessions/<session-id>/events \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: ev-2' \
  -d '{"expectedSeq":1,"event":{"type":"PAGE_REACHED","occurredAt":"2026-08-25T10:25:00Z","payload":{"page":42}}}'

# 查看账本
curl -sS localhost:8080/sessions/<session-id>
```

## 验收命令

```bash
docker compose up -d db      # 启动数据库
go mod download              # 下载依赖
go test ./...                # 全部测试（含并发竞争、幂等重放、非法状态迁移、append-only 强制）
go build ./...               # 构建全部包
go run ./cmd/server          # 启动服务
go run ./cmd/seed            # 可选：确定性种子
```

测试覆盖：

- `TestFullSessionLifecycleAndSummary`：完整事件流 → 阅读 48 分钟、最后页码 30、
  打断 2 次、感受原文返回。
- `TestIdempotencyReplay`：幂等重放返回首次响应原文、不二次落库；同键不同体 409。
- `TestConcurrentAppendsOnlyOneWins`：24 个并发追加恰好 1 个成功、23 个
  `SEQUENCE_CONFLICT` 且带当前序号。
- `TestIllegalStateTransitions`：开始前到达页码、重复开始、结束后追加、时间倒退、
  页码越界、序号错误、资源不存在等稳定错误码。
- `TestEventsTableAppendOnly`：直接对 `events` 执行 UPDATE/DELETE 被触发器拒绝。
- `TestSeedFixturesDeterministic`：种子夹具固定、可重复执行、汇总结果确定。
