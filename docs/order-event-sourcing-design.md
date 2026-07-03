# Order Service Event Sourcing Design

> 狀態：Phase 1 infrastructure implemented；BUY/SELL write path 尚未切換  
> 最後更新：2026-06-26  
> 範圍：只將 Order lifecycle 改為 Event-Sourced Aggregate；Wallet 與 Match Engine 暫不改為 Event Sourcing。

## 0. 為什麼會走到 Event Sourcing

這個設計不是從「我想在專案裡使用 Event Sourcing」開始，而是從一次容量驗證與 Audit correctness 問題逐步推導出來。

### 0.1 原本的 Order write path

原本 BUY / SELL request：

```text
HTTP request
-> backpressure check
-> allocate market sequence
-> publish OrderSubmittedEvent
-> wait RabbitMQ publisher confirm
-> AuditService.record(ORDER_SUBMITTED)
-> HTTP success
```

Wallet 處理完後，Order listener 又同步執行：

```text
OrderConfirmedEvent / OrderFailedEvent
-> SELECT latest audit FOR UPDATE
-> compute next hash
-> INSERT audit event
-> update orders read table
```

當時有兩套相近但不同的資料：

```text
orders / wallet / Redis state = 業務狀態來源
audit_events                  = 事後複製的 lifecycle history
```

Audit 不是 source of truth，但它和正式 request / listener 共用 Order Hikari pool，而且每個 lifecycle event 都同步寫入。

### 0.2 短測看起來沒有問題

Outbox Relay 改成 pipelined confirms + bulk SENT update 後，10,000 筆短測：

```text
actual TPS              = 992.64
p95                     = 120.02 ms
p99                     = 219.22 ms
wallet queue depth peak = 3,913
queue/outbox final      = 0 / 0
publish failure         = 0
```

這一輪容易得到錯誤結論：「系統已經能承受 1,000 TPS。」但 RabbitMQ 能吸收十秒尖峰，不代表下游 transaction rate 可以長期追上。

### 0.3 Soak test 暴露持續容量問題

接著執行 10 分鐘、目標 1,000 TPS、600,000 attempts 的 soak test：

```text
accepted                   = 371,083
sustained accepted TPS     = 618.41
HTTP 503 backpressure      = 212,076
HTTP 429                   = 16,841
unexpected failure         = 0
wallet queue depth peak    = 10,721
Order Hikari active peak   = 20 / 20
Order Hikari pending peak  = 109
Wallet Hikari pending peak = 0
Order CPU peak             ~= 14.3%
outbox oldest pending      = 2 seconds
```

測後 RabbitMQ queue 與 outbox 都能清空，371,083 筆 accepted event 也全部完成 publisher confirm、Wallet consume 與 outbox publish；duplicate orderId、negative wallet、FAILED outbox 都是 0。

這些指標把問題定位得很清楚：

- CPU 很低，不是 Java 計算能力不足。
- Wallet pool 沒有 pending，不是 Wallet DB transaction 先飽和。
- Outbox oldest age 只有兩秒，Outbox Relay 不是長期瓶頸。
- Order pool 20 條全部占用，另有 109 threads 等 connection。
- Queue 超過 hard threshold 後，backpressure 正確回 503，保護系統沒有崩潰。

真正限制 sustained throughput 的是 Order synchronous DB workload。

### 0.4 先排除看似直覺的解法

#### 增加 Wallet consumer

Base consumer concurrency 從 4 調到 6，壓力期間自動擴到 7：

```text
wallet queue peak    4,994 -> 5,679
wallet outbox peak   6,736 -> 9,035
Order Hikari pending 4     -> 33
API p95              149ms -> 193ms
```

Order 與 Wallet 共用 PostgreSQL instance；增加 consumer 只增加 DB 競爭，因此還原。

#### 將 Wallet SELECT + UPDATE 合成 atomic UPDATE

Wallet transaction 平均時間沒有下降，queue peak 也沒有改善，因此還原。這證明少一條 SQL 不必然等於較高的端到端 throughput；row lock、outbox insert、WAL 與共享 DB contention 仍存在。

#### 只優化 Outbox

Pipelined publisher confirms 與 bulk SENT update 成功降低短測 queue/outbox 壓力，但 soak test 仍顯示 Order pool 飽和。優化是有效的，只是不是最後一個瓶頸。

### 0.5 Audit 為什麼成為瓶頸

每個 accepted order 至少產生兩筆 audit：

```text
ORDER_SUBMISSION_REQUESTED : initial INSERT
ORDER_CONFIRMED / FAILED   : latest lookup + INSERT
```

620 orders/s 約等於至少 1,240 audit events/s。Audit 和 HTTP command、Order MQ listeners 使用同一個 pool，所以 confirmed audit consumer 可以把 request thread 需要的 connections 用完。

Soak 後資料量：

```text
audit_events rows  ~= 942,000
heap size          = 491 MiB
index size         = 247 MiB
total size         = 738 MiB
```

已知 correlationId 的 latest query 在 warm cache 約 0.31 ms，並不是沒有 index 的全表掃描。瓶頸是整條同步 write path：lookup、row lock、JSONB INSERT、WAL、五個 index updates、commit，以及 connection pool contention。

其中全域 hash unique index 已達 112 MiB，但 observed scans 為 0；它仍在每次 INSERT 時增加 write amplification。

### 0.6 Audit correctness 同時有問題

舊流程先 publish MQ，再寫 `ORDER_SUBMITTED` audit。Wallet 很快時，`ORDER_CONFIRMED` 可能先抵達 Order listener，和 API thread 競爭成為第一筆 audit：

```text
錯誤可能：GENESIS -> ORDER_CONFIRMED -> ORDER_SUBMITTED
```

因此先完成第一階段修正：

```text
recordInitial(ORDER_SUBMISSION_REQUESTED)
-> GENESIS direct insert
-> partial unique index 保證每個 correlationId 只有一個 GENESIS
-> commit 後才 publish MQ
```

真實 PostgreSQL 驗證 100 筆訂單：wrong first event = 0、broken CONFIRMED link = 0、duplicate GENESIS = 0。同 orderId 提交兩次時，initial audit、confirmed audit、Wallet claim 都只有一筆。

但這只解決 initial ordering 與一個多餘 lookup。後續 event 仍需同步 lookup + insert，且 `SELECT latest row FOR UPDATE` 鎖住的是 tail row，不是整條 correlation chain；真正並發 append 仍有 fork 風險。

### 0.7 為什麼不是繼續補 Audit table

可以繼續做：移除不必要 index、增加 composite index、建立 chain-head table、Audit 專用 pool、durable audit writer。這些都能改善現況。

但 Order lifecycle 本身已經由 events 驅動：

```text
SubmissionRequested
Confirmed / Failed
PartiallyMatched / Matched
Cancelled
Settled
```

若繼續維護：

```text
業務 state tables
+ MQ lifecycle events
+ audit lifecycle history
```

就會長期保留三套相似資料，還要回答哪一套才是真相、如何修復不一致、如何 replay，以及 Audit 是否漏寫。

因此選擇讓 Order domain events 成為唯一 source of truth：

```text
Order Event Store       = 正式 lifecycle history
OrderAggregate          = command-side business rules
orders_current          = 可重建 query projection
Event Outbox            = 可靠 integration publication
security audit          = 只保留非 domain 操作
```

### 0.8 Event Sourcing 不是魔法效能優化

採用 Event Sourcing 的主要理由是：

- 消除 Order state 與 Audit history 的雙重事實來源。
- 將 state transition、version conflict、duplicate、replay 規則明確化。
- 讓 query projection 可以非同步、獨立擴充與重建。
- 將 Event Store append + Outbox 壓縮成固定、小型、可量測的 transaction。

它不保證天然比 CRUD 快。Event Store、Outbox、Projector、checkpoint、schema evolution 都增加系統複雜度；設計不當甚至會更慢。

效能改善來自：command transaction 不再同步維護 Audit + read model、projection 移出 request path、append-only write、expected-version concurrency，以及 workload isolation，而不是來自「使用了 Event Sourcing」這個名稱。

### 0.9 決策範圍與成功條件

只改 Order Service。Wallet 保留 relational transaction / ledger 邊界，Match Engine 保留 Redis order book 與 market sequencing，避免一次重寫整個平台。

成功條件：

```text
Order Event Store 是唯一 domain source of truth
eventId duplicate       = 0
aggregate version gap   = 0
chain fork              = 0
projection mismatch     = 0
Order Hikari pending    可回落且不長期飽和
queue/outbox final      = 0
600/700/800 TPS soak    逐級取得可重現結果
```

## 1. 決策摘要

Order Service 將不再把 `orders` row 或 `audit_events` 當成訂單生命週期的主要事實來源。新的唯一 source of truth 是 append-only Order Event Store：

```text
OrderSubmissionRequested
-> OrderAssetReservationConfirmed / OrderAssetReservationFailed
-> OrderPartiallyMatched / OrderMatched
-> OrderSettled / OrderCancelled
```

目前的 `orders` table 轉為 read projection，可以刪除並從 Event Store 重建。Audit hash chain 合併到 Order event stream，不再為同一生命週期另外同步寫一套 audit history。

Event Store append 與 Outbox 必須在同一個 PostgreSQL transaction；RabbitMQ 仍採 at-least-once，因此所有 integration consumer 必須保留 idempotency。

### 1.1 目前實作進度

已完成：

- Liquibase 建立 `order_event_store`、`order_stream_heads`、`order_event_outbox`、`orders_current`、`projection_checkpoints`。
- `OrderEventAppender` 使用 stream-head row lock 與 `expectedVersion` 執行 atomic append。
- Event Store、head update、integration outbox 在同一 transaction。
- `eventId` retry 回傳既有結果；相同 eventId 搭配不同 immutable content 會拒絕。
- 保存 canonical payload / metadata，確保 JSONB 正規化後仍可重算 SHA-256 chain。
- PostgreSQL integration tests 驗證 initial append、subsequent hash link、duplicate、stale version rollback、outbox failure rollback、24-thread 同版本競爭。
- `OrderAggregate` 已涵蓋 submission requested、asset reservation confirmed/failed、matched、cancelled 的狀態轉移。
- BUY / SELL command 已改由 `OrderEventSourcingService` append `OrderSubmissionRequestedV1`，並在同一 transaction 建立 `OrderSubmittedEvent` outbox row。
- Wallet confirmed / failed listener 已改為 append `OrderAssetReservationConfirmedV1` / `OrderAssetReservationFailedV1`，不再同步寫 legacy `orders` / `audit_events`。
- Match result listener 已改為 append `OrderMatchedV1`，同時保留既有 match history 與 WebSocket notification。
- `OrderEventOutboxRelay` 已啟用，負責把 `order_event_outbox` 中的 integration event 發布到 RabbitMQ。
- `orders_current` projector 已啟用，使用 `projection_checkpoints` 記錄處理到的 `global_position`。

尚未完成：

- 長時間 soak test 尚未重跑；目前只有小規模 E2E 驗證。
- Query API 仍需逐步盤點，確保訂單狀態查詢改讀 `orders_current` 或其他 projection。
- `OrderEventOutboxRelay` 目前以單 instance 為前提；多 pod 需要 claim / `FOR UPDATE SKIP LOCKED` 或等價機制。
- `orders_current` projector 需要補 restart、replay、poison-event 與 projection rebuild 測試。
- Cancel flow 目前仍先呼叫 Match Engine cancel，再 append cancel event；若要嚴格一致，應改成 command event + async cancel integration。

目前邊界是刻意的：先把 Order lifecycle 的主寫入路徑切到 Event Store + Outbox，確認業務流程不受影響；之後再補長時間壓測、多 instance relay safety、projection rebuild 工具與 cancel 流程一致性。

## 2. 重要名詞與表格關係

### Aggregate 不是資料表

`OrderAggregate` 是 Java domain object：

```text
歷史 events
-> apply(event)
-> 還原目前 state / version
-> handle(command)
-> 產生新的 domain events
```

它不必直接對應 `order_aggregate` 狀態表。如果把完整 Aggregate state 當成主表更新，就又回到 CRUD + event log 雙 source of truth。

### 建議資料表

| Table | 必要性 | 職責 |
|-------|--------|------|
| `order_event_store` | 必要 | Order domain events；唯一 source of truth |
| `order_stream_heads` | 建議 | 每個 order stream 的 version、last hash、鎖定點 |
| `order_event_outbox` | 必要 | Event Store commit 後可靠發布 integration event |
| `orders_current` | 必要 | 目前訂單狀態的 query projection |
| `projection_checkpoints` | 必要 | Projector 已處理到哪一個 global position |
| `order_snapshots` | 選配 | Event 很長時加速 Aggregate load；第一版不需要 |
| 其他 projection | 按查詢需要 | 例如 user open orders、market order history |

Projection 是依 query use case 設計，不是每一種 event type 建一張表。

## 3. Target Architecture

```text
REST / MCP Command
       |
       v
Order Application Service
       |
       +-> load events / snapshot
       +-> OrderAggregate.handle(command)
       +-> append events with expectedVersion
              |
              +-- same DB transaction --> order_event_store
              +-- same DB transaction --> order_stream_heads
              +-- same DB transaction --> order_event_outbox
                                             |
                                             v
                                      Outbox Relay + confirm
                                             |
                                      RabbitMQ integration events
                                             |
              +------------------------------+--------------------+
              |                              |                    |
           Wallet                       Match Engine        Order Projector
                                                                  |
                                                                  v
                                                           orders_current
```

Wallet 或 Match Engine 不可以直接寫 Order Event Store。它們發布 integration event，Order Service 將 integration event 轉為 command，再由 Aggregate 產生自己的 domain event。

範例：

```text
WalletAssetReserved integration event
-> ConfirmAssetReservation command
-> OrderAggregate validates current state
-> OrderAssetReservationConfirmed domain event
-> Event Store
```

## 4. Domain Events 與 Integration Events

兩者不應混為同一個 DTO。

### Domain event

Order Service 內部事實：

```text
OrderSubmissionRequested
OrderAssetReservationConfirmed
OrderAssetReservationFailed
OrderPartiallyMatched
OrderMatched
OrderCancelled
OrderSettled
```

Domain event 必須足以重建 Order Aggregate。

### Integration event

對其他服務公開的 contract：

```text
OrderSubmittedEvent
OrderConfirmedEvent
OrderCancelledIntegrationEvent
```

Integration contract 可以與內部 domain event 分開演進。Outbox payload 應保存 integration event，而 Event Store 保存 domain event。

## 5. Order Aggregate State Machine

第一版狀態：

```text
NOT_CREATED
  -> PENDING_ASSET_CHECK
       -> OPEN
       -> REJECTED

OPEN
  -> PARTIALLY_MATCHED
  -> MATCHED
  -> CANCELLED

PARTIALLY_MATCHED
  -> PARTIALLY_MATCHED
  -> MATCHED
  -> CANCELLED_REMAINDER

MATCHED
  -> SETTLED
  -> SETTLEMENT_FAILED
```

Command handler 只能根據 Aggregate current state 決定是否產生 event：

```text
ConfirmAssetReservation on PENDING_ASSET_CHECK -> OrderAssetReservationConfirmed
ConfirmAssetReservation on OPEN                -> idempotent no-op / duplicate
ConfirmAssetReservation on CANCELLED           -> illegal transition
```

事件 replay 的 `apply()` 不重新驗證 command，也不執行外部 IO，只負責 deterministic state transition。

## 6. Event Store Schema

概念 schema：

```sql
CREATE TABLE order_service.order_event_store (
    global_position  bigserial PRIMARY KEY,
    event_id         uuid NOT NULL UNIQUE,
    aggregate_id     uuid NOT NULL,
    aggregate_type   varchar(50) NOT NULL,
    aggregate_version bigint NOT NULL,
    event_type       varchar(100) NOT NULL,
    payload          jsonb NOT NULL,
    metadata         jsonb NOT NULL,
    schema_version   integer NOT NULL,
    occurred_at      timestamp NOT NULL,
    prev_hash        varchar(64) NOT NULL,
    hash             varchar(64) NOT NULL,
    UNIQUE (aggregate_id, aggregate_version)
);

CREATE INDEX idx_order_event_stream
ON order_service.order_event_store(aggregate_id, aggregate_version);
```

欄位意義：

- `event_id`：事件身份與 at-least-once idempotency。
- `aggregate_id`：orderId，代表哪一條 stream。
- `aggregate_version`：此事件在 Order Aggregate 的順序與 optimistic concurrency token。
- `global_position`：Projector 跨所有 Aggregate 的 checkpoint 順序。
- `schema_version`：舊事件 payload 升版用。
- `metadata`：correlation ID、causation ID、trace ID、source message ID、actor。
- `prev_hash/hash`：保留 tamper-evident audit 能力；Event Sourcing 本身不強制需要 hash。

`event_id` 可以作為 Event Store PK；但仍建議保留遞增 `global_position` 作為 physical PK / projector cursor。若 UUID 是 PK，B-tree locality 與全域 replay checkpoint 會較差。第一版可採 `global_position` PK + `event_id UNIQUE`。

## 7. Stream Head Schema

```sql
CREATE TABLE order_service.order_stream_heads (
    aggregate_id       uuid PRIMARY KEY,
    current_version    bigint NOT NULL,
    last_event_id      uuid NOT NULL,
    last_hash          varchar(64) NOT NULL,
    updated_at         timestamp NOT NULL
);
```

這不是 Aggregate state table，只是：

- Per-order serialization point。
- Expected version check。
- Last hash lookup。
- 避免從持續成長的 Event Store 找 tail。

完整 Order state 仍由 events 重建；`orders_current` 才是查詢用的狀態投影。

## 8. Atomic Append

Application 呼叫：

```text
append(orderId, expectedVersion, newEvents)
```

Transaction：

```text
1. SELECT order_stream_heads WHERE aggregate_id=? FOR UPDATE
2. 驗證 current_version == expectedVersion
3. event_id 已存在則判斷為 duplicate
4. 按順序 INSERT domain events
5. UPDATE stream head version / last hash
6. INSERT integration events 到 outbox
7. COMMIT
```

兩個 command 同時嘗試從 version 2 寫 version 3：

```text
Command A -> append v3 success
Command B -> expectedVersion conflict
          -> reload aggregate
          -> 重新判斷 command 或視為 duplicate
```

可以使用 PostgreSQL function 把 lock、event insert、head update 與 outbox insert包成一次 client round trip，但 domain decision 仍留在 Java Aggregate。

## 9. Outbox 與 Publication

```sql
CREATE TABLE order_service.order_event_outbox (
    id              bigserial PRIMARY KEY,
    event_id        uuid NOT NULL UNIQUE,
    aggregate_id    uuid NOT NULL,
    routing_key     varchar(100) NOT NULL,
    payload         jsonb NOT NULL,
    status          varchar(20) NOT NULL,
    attempt_count   integer NOT NULL,
    next_retry_at   timestamp,
    last_error      text,
    created_at      timestamp NOT NULL,
    published_at    timestamp
);
```

沿用 Wallet Outbox 已完成的：

- Bounded batch。
- Pipelined publisher confirms。
- Mandatory returns。
- Backoff / FAILED / recovery。
- Metrics / alerts。

Event Store commit 後 API 可以宣告 command 已接受；RabbitMQ publish 是可靠但非同步。因此 API response 應包含：

```json
{
  "orderId": "...",
  "aggregateVersion": 1,
  "status": "PENDING_ASSET_CHECK"
}
```

## 10. Projection Tables

### Current Order Projection

```sql
CREATE TABLE order_service.orders_current (
    order_id          uuid PRIMARY KEY,
    user_id           uuid NOT NULL,
    market_id         varchar(50) NOT NULL,
    market_sequence   bigint NOT NULL,
    side              varchar(4) NOT NULL,
    price             integer NOT NULL,
    original_amount   integer NOT NULL,
    remaining_amount  integer NOT NULL,
    matched_amount    integer NOT NULL,
    status            varchar(30) NOT NULL,
    aggregate_version bigint NOT NULL,
    created_at        timestamp NOT NULL,
    updated_at        timestamp NOT NULL
);
```

Projector 必須：

- 依 `global_position` 處理。
- 對相同 event 重跑保持冪等。
- 只接受比 projection current version 更新的 aggregate version。
- 不發布 MQ、不扣 Wallet、不呼叫外部 API。

### Checkpoint

```sql
CREATE TABLE order_service.projection_checkpoints (
    projection_name varchar(100) PRIMARY KEY,
    last_global_position bigint NOT NULL,
    updated_at timestamp NOT NULL
);
```

事件處理與 checkpoint 更新必須在同一個 projection transaction。

### 是否每種類型一張 projection？

不是。依查詢模式新增：

- `orders_current`：依 orderId 查目前狀態。
- `user_open_orders`：只有使用者 open-order query 真有壓力時才建立。
- `market_order_history`：市場歷史查詢需要時建立。

能由 `orders_current` + index 有效回答的查詢，不額外維護 projection。

## 11. Snapshot

Order lifecycle 通常只有數個至數十個 events，第一版不需要 snapshot。直接 replay stream 成本很低。

當單一 Order 可能有大量 partial fill events，再加入：

```sql
CREATE TABLE order_service.order_snapshots (
    aggregate_id uuid PRIMARY KEY,
    aggregate_version bigint NOT NULL,
    state jsonb NOT NULL,
    schema_version integer NOT NULL,
    created_at timestamp NOT NULL
);
```

Load：

```text
latest snapshot
-> replay events after snapshot version
```

Snapshot 是 cache，可以刪除重建，不能取代 Event Store。

## 12. Replay 與 Rebuild

單一訂單：

```sql
SELECT *
FROM order_service.order_event_store
WHERE aggregate_id = ?
ORDER BY aggregate_version;
```

全 projection rebuild：

```text
建立 orders_current_rebuild
-> 從 global_position 0 replay
-> 驗證 row count / checksum / lag
-> atomic rename 或切換 read alias
```

不要直接清空 production projection 後原地重建，否則重建期間 query 無法使用。

Replay safety：

- `apply()` 不執行外部 side effect。
- Projector 不發布 integration event。
- Email/WebSocket/MQ 由 live subscriber 處理，不在 rebuild 執行。

## 13. Event Schema Evolution

事件不可像普通 DTO 一樣直接修改舊 schema。

方法：

- Event name 明確版本化，例如 `OrderSubmittedV1`。
- 保存 `schema_version`。
- 讀取時使用 upcaster 將舊 payload 轉為目前 domain event。
- 不直接 UPDATE 歷史 event payload。

需要測試：所有歷史 schema version 都能 replay 到目前 Aggregate。

## 14. Ordering 與 Cross-Aggregate Event

`aggregate_version` 只保證單一 Order 順序。撮合事件同時影響 buyer order 與 seller order：

- 兩個 Order Aggregate 各自 append 一筆 `OrderMatched`。
- 使用同一 `matchId` 作為 causation metadata / idempotency key。
- 不嘗試建立跨兩個 Aggregate 的單一 version。

Market price-time ordering 繼續由 `marketSequence` 與 Match Engine 負責；它和 Order Aggregate version 是不同維度：

```text
marketSequence    = 市場全域接單順序
aggregateVersion = 單一訂單生命週期順序
```

跨 Aggregate 強一致 transaction 會大幅提高耦合；EAP 維持 Saga / integration event + idempotency。

## 15. Consistency Model

Command side 與 query side 是 eventual consistency：

```text
POST /orders -> Event Store commit -> HTTP success
Projection 可能晚幾十毫秒才更新
GET /orders/{id} 可能短暫看不到新狀態
```

處理方式：

- Command response 直接回傳 orderId、version、由 Aggregate 得到的狀態。
- Query API 可接受 `minimumVersion`，projection 未追上時回 pending / retry。
- 監控 projection lag：`latest_global_position - checkpoint`。

不要為了 read-your-write 在 request 內同步更新 projection，否則重新引入 dual-write 與同步 DB 瓶頸。

## 16. Audit 與 Event Sourcing 的關係

Domain Event Store 已能提供 Order lifecycle audit，但 security/compliance audit 與 domain event 不完全相同：

Event Store 保存：

- Domain state changes。
- Actor / correlation / causation metadata。
- Integrity hash。

獨立 security audit 可保存：

- 登入、權限拒絕、管理操作。
- 查詢敏感資料。
- 不會改變 Order Aggregate 的操作。

不要再把 Order domain event 同步複製到舊 `audit_events`，否則重新造成兩套歷史與 DB write amplification。

## 17. 優點

- Order lifecycle 有單一 source of truth。
- Aggregate state transition 可測試且集中。
- Duplicate / concurrency 由 eventId + expectedVersion 處理。
- Audit history、debug 與 replay 能力強。
- Projection 可針對查詢獨立演進與重建。
- Event append 是固定的小型 transaction，不再同步更新多個 read tables。
- Wallet Outbox 經驗可以直接重用。

## 18. 缺點與成本

- 架構與除錯心智負擔高於 CRUD。
- Query 必須依賴 projection，會有 eventual consistency。
- Event schema 永久存在，需要版本與 upcaster。
- Event Store 不能任意 delete/update，storage 持續成長。
- Replay、projection rebuild、snapshot、checkpoint 都要有操作工具。
- Event ordering、duplicate、poison event 需要明確策略。
- 對簡單 Order CRUD 可能是 over-engineering；本專案採用的理由是 lifecycle、Saga、audit 與系統設計學習價值，而不是單純追求 TPS。

## 19. 失敗模式

| 失敗 | 處理 |
|------|------|
| Event append rollback | Command 失敗，沒有狀態變更 |
| Event commit、MQ 尚未發布 | Outbox retry |
| MQ duplicate delivery | eventId / source message idempotency |
| Projection crash | 從 checkpoint 繼續 |
| Projection poison event | 停止 checkpoint、告警、修正 upcaster/projector |
| Concurrent command | expectedVersion conflict，reload/retry |
| Out-of-order integration event | Aggregate state validation；暫存/retry/DLQ |
| Event schema 不相容 | upcaster + replay test 阻止部署 |
| Hash 驗證失敗 | 停止 stream/rebuild，觸發 integrity alert |

## 20. Migration Strategy

不建議長期 dual-write 舊 `orders`、舊 audit 與新 Event Store，因為三份資料難以證明一致。

本專案建議：

### Phase 0：Freeze Contract

- 列出 Order command、domain event、integration event、state machine。
- 定義 event metadata 與 schema version。

### Phase 1：Infrastructure

- 建立 Event Store、stream head、outbox、projection、checkpoint schema。
- 實作 append primitive、repository、upcaster interface。
- 完成 concurrent append / duplicate / rollback tests。

狀態：已完成。

### Phase 2：New Order Write Path

- BUY/SELL 改為建立 `OrderAggregate`。
- Append `OrderSubmissionRequestedV1` + outbox。
- Wallet confirmed/failed listeners 改為 Aggregate command handlers。
- 舊 `audit_events` 不再接收 Order domain lifecycle。

狀態：主路徑已完成。BUY/SELL、Wallet confirmed/failed、Match result 已切換到 Event Store；legacy `orders` / `audit_events` 不再承接新 Order lifecycle 寫入。

### Phase 3：Projection

- `orders_current` projector。
- Query API 切換到 projection。
- 增加 lag dashboard、rebuild command。

狀態：`orders_current` projector 與 checkpoint 已完成；Query API 盤點、lag dashboard、rebuild command 尚未完成。

### Phase 4：Existing Data

由於本機資料主要是 load-test data，可清空後從新 Event Store 開始。若模擬 production migration：

- 每個既有 order 建立 `OrderImportedV1`，包含 migration 時狀態。
- 不偽造未知的完整歷史。
- 保存 migration metadata 與來源 row checksum。

### Phase 5：Remove Legacy

- 停止舊 Order audit dual-write。
- 將舊 `orders` 定義為 projection 或移除。
- 保留只讀 legacy audit archive，直到驗證期結束。

狀態：新寫入路徑已停止主要 Order lifecycle dual-write；legacy table 仍保留為歷史資料與相容期。

## 21. Verification Plan

### Correctness

- Aggregate state transition unit tests。
- Same eventId duplicate append 只產生一筆。
- 24+ threads append same expectedVersion 只有一個成功。
- Version 無 gap、無 duplicate。
- Hash chain 可重算。
- Event Store + outbox transaction rollback test。
- Projection duplicate/restart/rebuild test。
- Historical schema replay test。

### Capacity

使用 bounded generator：

```text
600 TPS × 10 minutes
700 TPS × 10 minutes
800 TPS × 10 minutes
必要時 1,000 TPS × 10 minutes
```

指標：

- Event append p50/p95/p99。
- Expected-version conflicts。
- Outbox pending / oldest age。
- Projection lag。
- Order Hikari active / pending。
- RabbitMQ queue depth。
- Event Store rows/s、WAL、index growth。
- Heap / GC / CPU。

驗收：

```text
unexpected failure          = 0
event loss                  = 0
duplicate eventId           = 0
duplicate aggregate version = 0
projection mismatch         = 0
chain fork                  = 0
queue/outbox final          = 0
```

### 2026-06-25 小規模 E2E 驗證結果

測試範圍：

```text
HTTP Order API
-> Order Event Store append
-> Order integration outbox
-> RabbitMQ
-> Wallet validation / reservation
-> Wallet confirmed event
-> Order Event Store asset-reservation-confirmed append
-> orders_current projection
```

測試結果：

```text
orders submitted            = 100
accepted                    = 100
http429/http503/failures    = 0 / 0 / 0
event_store rows            = 200
OrderSubmissionRequestedV1  = 100
OrderAssetReservationConfirmedV1 = 100
stream heads at version 2   = 100
orders_current OPEN v2      = 100
order_event_outbox SENT     = 100
projection checkpoint       = 206 / 206
duplicate aggregate version = 0
broken hash links           = 0
missing parent links        = 0
```

舊表驗證：

```text
audit_events    = 942370
legacy orders   = 471184
event_store     = 200
projection_rows = 100
```

這代表新 100 筆訂單沒有再把 Order lifecycle 同步複製到 legacy `audit_events` / `orders`。目前證據足以證明切換後的 happy path 與基本完整性成立，但還不足以宣稱長時間容量已完成；下一步需要重跑 10 分鐘以上 soak test，並觀察 event outbox pending、oldest pending age、projection lag、Order Hikari pending 與 RabbitMQ queue depth。

### 2026-06-25 5 分鐘 soak test 結果

參數：

```text
duration      = 300 seconds
target TPS    = 600
workers       = 128
users         = 500
total requests= 180,000
```

HTTP 結果：

```text
accepted      = 128,598
http429       = 0
http503       = 51,402
otherFailures = 0
elapsed       = 329.91 s
actual TPS    = 389.80 accepted/s
p50           = 81.73 ms
p95           = 403.41 ms
p99           = 767.87 ms
```

最終一致性檢查：

```text
OrderSubmissionRequestedV1        = 128,698
OrderAssetReservationConfirmedV1  = 128,698
orders_current OPEN v2            = 128,698
order_event_outbox pending        = 0
projection checkpoint             = 257408 / 257408
RabbitMQ queues                   = all drained to 0
duplicate aggregate version       = 0
broken hash links                 = 0
missing parent links              = 0
legacy audit_events               = 942370 unchanged
legacy orders                     = 471184 unchanged
```

解讀：

- 原本 `audit_events` / legacy `orders` 同步雙寫造成的 DB 寫入放大沒有回來；新訂單沒有再寫入 legacy lifecycle table。
- Event Store append、Order outbox、projection checkpoint 的最終一致性成立。
- 600 TPS 持續送單時仍會觸發大量 503，這是 admission control 正常保護下游，不是資料遺失。
- 中途觀察到 `order.orderConfirmed.queue` 可堆到 100k+，主要瓶頸轉移到 Order confirmed listener：目前只有 1 個 consumer 在把 Wallet confirmed integration event 轉成 `OrderAssetReservationConfirmedV1`。
- 停止送單後 queue 可 drain 到 0，表示系統可恢復，但目前穩態吞吐上限低於 600 TPS。

下一步若要把 600 TPS 變成穩態：

1. 提高 `order.orderConfirmed.queue` consumer concurrency，並確認同一 order stream 的 expected-version conflict / idempotency 行為。
2. 為 `OrderEventOutboxRelay` 與 confirmed listener 補多 instance safety，避免 scale-out 後重複 publish 或同 stream 競爭失控。
3. 補 confirmed listener 的處理 latency 指標，拆出 RabbitMQ wait、Event Store append、projection lag。
4. 再重跑 600 TPS / 10 分鐘；驗收條件是 503 顯著下降、queue 不持續成長、final outbox/projection/hash check 仍為 0 異常。

### 2026-06-25 confirmed consumer concurrency 修正後驗證

修正內容：

- `order.orderConfirmed.queue` listener concurrency 在 loadtest profile 提高到 16。
- `order.orderFailed.queue` listener concurrency 在 loadtest profile 提高到 4。
- `OrderEventSourcingService` append 的 `occurredAt` 改用 integration event 內可重現的 timestamp，避免同一 message 並行重送時因每次 `now()` 不同而觸發 event identity conflict。
- Event Store domain event 名稱改為 `OrderAssetReservationConfirmedV1` / `OrderAssetReservationFailedV1`，避免把 Wallet 資產保留成功誤解成「訂單成交確認」。

測試前已清空新 Event Store 測試資料：

```text
order_event_store
order_event_outbox
order_stream_heads
orders_current
projection_checkpoints
```

RabbitMQ listener 確認：

```text
order.orderConfirmed.queue consumers = 16
order.orderFailed.queue consumers    = 4
```

同樣使用 600 TPS × 300 秒測試：

```text
accepted      = 84,173
http429       = 0
http503       = 95,827
otherFailures = 0
elapsed       = 380.21 s
actual TPS    = 221.38 accepted/s
p50           = 2.94 ms
p95           = 596.08 ms
p99           = 1116.22 ms
```

最終一致性檢查：

```text
OrderSubmissionRequestedV1        = 84,173
OrderAssetReservationConfirmedV1  = 84,173
orders_current OPEN v2            = 84,173
order_event_outbox pending        = 0
projection checkpoint             = 168346 / 168346
RabbitMQ queues                   = all drained to 0
duplicate aggregate version       = 0
broken hash links                 = 0
missing parent links              = 0
```

對比前一輪：

```text
before: order.orderConfirmed.queue peaked at 100k+
after : order.orderConfirmed.queue stayed in low thousands and drained quickly
```

解讀：

- 這次修正有效解掉 Order confirmed listener 的單 consumer 瓶頸。
- 600 TPS 仍未成為穩態，因為瓶頸轉移到 `wallet.orderSubmitted.queue`：中途觀察到該 queue 維持在約 10k ready，觸發 Order backpressure 回 503。
- `order_event_outbox` 沒有 pending，代表 Order request append 與 integration outbox relay 不是目前瓶頸。
- Projection 最終可追到最新，但中途 lag 明顯；後續若查詢壓力提高，需要單獨評估 projector batch/concurrency/rebuild 策略。

下一步不要再優先調 Order confirmed consumer；應處理 Wallet 消費吞吐或 backpressure 策略：

1. 測 Wallet `wallet.orderSubmitted.queue` consumer concurrency / prefetch / DB transaction latency。
2. 觀察 Wallet Hikari active/pending、wallet lock/update latency、wallet outbox insert latency。
3. 若 Wallet DB 未飽和，再提高 Wallet consumer；若 DB 飽和，先降 transaction round trips 或隔離 DB 資源。
4. 再重跑 600 TPS / 10 分鐘，驗收 queue 不持續成長且 final Event Store / projection / hash check 仍乾淨。

### 2026-06-25 Wallet consumer 調整與 projector 修正

Wallet 問題定位：

- 前一輪 600 TPS 測試中，`wallet.orderSubmitted.queue` 維持在約 10k ready，觸發 Order backpressure。
- Wallet loadtest profile 實際 consumer 只有約 5 個，且 Hikari max pool 為 10。
- Wallet 測試表累積大量歷史資料：`wallet_service.outbox` 約 300MB、`order_submission_idempotency` 約 101MB，會污染壓測結果。

修正：

- `CreateOrderListener` 增加專用 listener concurrency：

```text
eap.wallet.listeners.order-submitted.concurrency
```

- loadtest profile 設定：

```text
wallet.orderSubmitted.queue consumers = 16
Wallet Hikari max pool                = 30
Wallet outbox relay batch             = 500
Wallet outbox relay poll interval     = 100ms
```

- 測試前清空 Wallet load-test data：

```text
wallet_service.outbox
wallet_service.order_submission_idempotency
wallet_service.wallets
```

中途驗證結果：

```text
target TPS    = 600
accepted      = 92,270 before manual stop
http503       = 0
wallet.orderSubmitted.queue = no sustained backlog
wallet_outbox_pending       = 0
```

解讀：

- Wallet 吞吐瓶頸已明顯改善，之前卡在 10k backpressure threshold 的問題不再出現。
- 因為 Wallet 變快，壓力往下游推進到 Order Event Store：中途觀察到 `order.orderConfirmed.queue` 累積，Order Hikari 曾達 `active=20/20, pending=125`。

同時暴露 projector bug：

- `orders_current` projector 原本假設 `OrderSubmissionRequestedV1` 一定已投影，才會處理 `OrderAssetReservationConfirmedV1`。
- 在高併發與 checkpoint batch 下，若後續事件處理時找不到 `orders_current` row，projector 會在同一 global position 反覆 rollback，checkpoint 卡死。

修正：

- 當 projector 處理後續事件時發現 `orders_current` row 不存在，會從該 order 的 event stream 重建 projection 到目前 event version，upsert `orders_current` 後繼續。
- 這維持 projection 可重建、idempotent、eventual consistent 的設計。

修正後 drain 驗證：

```text
wallet.orderSubmitted.queue           = 0
order.orderConfirmed.queue            = 0
OrderSubmissionRequestedV1            = 96,141
OrderAssetReservationConfirmedV1      = 96,141
orders_current OPEN v2                = 96,141
order_event_outbox pending            = 0
wallet_outbox_pending                 = 0
projection checkpoint                 = 192282 / 192282
```

目前不能宣稱 600 TPS 穩態已完成，因為這輪測試在定位 Order DB pool/projector 問題後手動停止。下一輪要清資料後完整重跑 600 TPS × 5～10 分鐘，才可作為穩態容量證據。

### 2026-06-25 Order / Wallet write-path 調校結論

前一輪已證明 projector 卡死可修復，但重新跑 600 TPS 短測後，瓶頸從單一 consumer 轉成 shared DB write path。

#### 調整內容

Order Event Store append：

- 正常新事件不再先查 `event_id`。`OrderEventAppender` 先走 expected-version fast path，只有 version conflict 或 duplicate key 時才回查 `event_id` 判斷是否為 idempotent redelivery。
- `expectedVersion > 0` 的事件不再執行 `INSERT order_stream_heads ... ON CONFLICT DO NOTHING`，避免 Wallet confirmed / failed hot path 多一次無效 write attempt。

Order loadtest profile：

```text
order.orderConfirmed.queue consumers = 16
order.orderFailed.queue consumers    = 4
order-projection batch-size          = 500
order-projection poll-interval-ms    = 5000
Order Hikari max pool                = 50
```

Wallet write path：

- `CreateOrderListener.recordOrderSubmitted()` 從 `saveAndFlush()` 改成 `save()`。
- 設計理由：正常壓測沒有 duplicate，提前 flush idempotency claim 會增加每筆 wallet transaction 的 DB round trip；改成 commit flush 後，duplicate 仍會由 unique constraint 在 commit 時丟 `DataIntegrityViolationException` 並 rollback。

Wallet balanced loadtest profile：

```text
wallet.orderSubmitted.queue consumers = 32
Wallet Hikari max pool                 = 40
Wallet outbox relay batch              = 500
Wallet outbox relay poll interval      = 100ms
```

#### 600 TPS / 120 秒短測結果

修正前基準：

```text
target      = 600 TPS / 120s
accepted    = 68,406 / 72,000
http503     = 3,594
actual TPS  = 348.37
p95         = 596.73ms
p99         = 799.13ms
```

Order appender fast path + Wallet balanced profile：

```text
target      = 600 TPS / 120s
accepted    = 66,293 / 72,000
http503     = 5,707
actual TPS  = 398.36
p95         = 500.77ms
p99         = 699.88ms
```

解讀：

- `event_id` 預查不再出現在 hot query，Order append path 有效減少 DB read round trip。
- 實際入口吞吐從約 345 TPS 提升到約 398 TPS，p95 / p99 latency 下降。
- 600 TPS 仍會讓 `wallet.orderSubmitted.queue` 靠近 10k backpressure threshold，後段仍會回 503。
- `order.orderConfirmed.queue` 也會累積，表示 Wallet confirmed integration event 轉 Order domain event 的寫入仍追不上 600 TPS。

#### 被撤回的調整

```text
wallet.orderSubmitted.queue consumers = 48
Wallet Hikari max pool                = 70
```

結果：PostgreSQL 回 `too many clients`，TPS 掉到約 180～240。這證明 consumer / pool 不能無限制增加。

```text
wallet.orderSubmitted.queue consumers = 40
Wallet Hikari max pool                = 40
```

結果：wallet queue 幾乎不堆，但 async consumer 吃滿 shared DB connections，HTTP entry path 變慢。這不是可接受的穩態調校。

#### 架構結論

目前 600 TPS 不是單純調參問題。Order HTTP append、Order confirmed append、Wallet reservation transaction、Wallet outbox relay、Order projection 都共用同一個 PostgreSQL 寫入能力與連線上限。加 consumer 只是在不同 queue 之間搬移瓶頸。

下一步若要把 600 TPS 變成穩態，優先做 workload isolation：

1. Order HTTP append、confirmed listener、projection 使用獨立 datasource / pool，保留入口連線預算，避免 async consumer 壓垮接單。
2. Projection 拆到獨立 worker / pod，或在高峰期降低 projection priority。
3. Order DB 與 Wallet DB 分離，避免兩個 bounded context 共用同一個 PostgreSQL `max_connections` / WAL。
4. Event Store append 可演進成 PostgreSQL function 或批次 writer，減少 lock head、insert event、update head、insert outbox 的 client round trips。
5. 只有在下游容量確認足夠後，才調高 wallet queue backpressure threshold；否則只是把失敗從 503 變成更長延遲與更大 backlog。

### 2026-06-25 Phase 1：Order service 內部分 pool 實測

目的：驗證不拆 DB 的情況下，是否能靠 Order service 內部 workload isolation 保護 HTTP command path，避免 confirmed listener / projection 搶光同一個 Hikari pool。

#### 實作

新增三組 pool：

```text
OrderCommandPool
- primary datasource
- JPA transaction manager
- HTTP command path
- request/cancel append

OrderConsumerPool
- Wallet confirmed/failed listener
- match listener append
- Order event outbox relay

OrderProjectionPool
- orders_current projector only
```

loadtest profile：

```text
OrderCommandPool max     = 35
OrderConsumerPool max    = 12
OrderProjectionPool max  = 3
Order total max          = 50
Wallet Hikari max        = 40
```

#### 測試結果

600 TPS / 120 秒：

```text
accepted    = 72,000 / 72,000
http503     = 0
actual TPS  = 330.66
p50         = 387.71ms
p95         = 602.73ms
p99         = 897.67ms
```

最終一致性：

```text
OrderSubmissionRequestedV1       = 72,000
OrderAssetReservationConfirmedV1 = 72,000
orders_current OPEN v2           = 72,000
projection checkpoint            = 144000 / 144000
RabbitMQ queues                  = drained to 0
```

#### 解讀

分 pool 有效做到 isolation：

- HTTP 入口沒有 503。
- Wallet queue 沒有長時間卡住。
- confirmed queue / projection lag 停止送單後可 drain。

但它沒有提升吞吐：

- actual TPS 只有 330.66，低於分 pool 前 fast-path + balanced profile 的約 398 TPS。
- 中途 `pg_stat_activity` 顯示大量 active session 等待 `WALWrite`。
- 這表示當前限制已經不是「誰搶 Hikari connection」而是單一 PostgreSQL instance 的 WAL write capacity。

工程結論：

- Phase 1 是隔離策略，不是擴容策略。
- 它適合用來保護重要路徑，避免 async worker 壓垮 HTTP 入口。
- 但在單 DB / shared WAL 下，Order HTTP append、Order confirmed append、Wallet reservation、outbox relay、projection 的寫入仍然會匯聚到同一個 WAL bottleneck。
- 若目標是穩定接近 600 TPS，下一個有效方向是 Phase 3：Order DB / Wallet DB 分離，或提升 PostgreSQL 寫入/WAL I/O 能力。繼續微調 pool 比例只是在入口吞吐、confirmed 延遲、projection lag 之間取捨。

### 2026-06-26 `synchronous_commit=off` 診斷

目的：驗證 Phase 1 後看到的 `WALWrite` wait event 是否真的是主要瓶頸之一。

這不是正式設計變更。`synchronous_commit=off` 會讓 transaction commit 不等待 WAL flush 到 durable storage，crash 時可能遺失最近已回應成功的 transaction。因此它只適合用來診斷 WAL fsync 成本，不適合作為目前交易系統的預設可靠性策略。本輪測完已還原成 `synchronous_commit=on`。

#### 測試條件

```text
target                  = 600 TPS / 120 秒
requests                = 72,000
OrderCommandPool max    = 35
OrderConsumerPool max   = 12
OrderProjectionPool max = 3
Wallet Hikari max       = 40
projection enabled      = true
synchronous_commit      = off during test only
```

#### 結果

```text
accepted    = 72,000 / 72,000
http503     = 0
http429     = 0
failures    = 0
actual TPS  = 371.57
p50         = 307.77ms
p95         = 579.83ms
p99         = 715.93ms
```

Phase 1 baseline：

```text
synchronous_commit = on
actual TPS         = 330.66
p50                = 387.71ms
p95                = 602.73ms
p99                = 897.67ms
```

對比結果：

```text
TPS improvement = 330.66 -> 371.57 ~= +12.4%
p50 improvement = 387.71ms -> 307.77ms
p99 improvement = 897.67ms -> 715.93ms
```

#### 中途觀察

`synchronous_commit=on` 時：

```text
pg_stat_activity active sessions: many waiting WALWrite
```

`synchronous_commit=off` 時：

```text
WALWrite wait 消失
部分 active sessions 轉為 BufferContent / active transaction 壓力
wallet.orderSubmitted.queue 很快清空
order.orderConfirmed.queue 仍會累積，但停止送單後可 drain
projection checkpoint 會落後，但可追上
```

這代表關掉 commit-time WAL flush 後，系統不是直接到 600 TPS，而是把瓶頸往後推到：

1. Order confirmed listener 把 Wallet confirmed integration event 轉成 Order domain event 的 append throughput。
2. Event Store append transaction 的資料頁 / index / stream-head 更新競爭。
3. 低優先級 projection 追趕速度。

#### 最終一致性

```text
RabbitMQ queues                  = drained to 0
OrderSubmissionRequestedV1       = 72,000
OrderAssetReservationConfirmedV1 = 72,000
orders_current OPEN v2           = 72,000
projection checkpoint            = 144000 / 144000
duplicate aggregate versions     = 0
broken hash links                = 0
```

#### 結論

`synchronous_commit=off` 證明 WAL fsync 是瓶頸之一，但不是唯一瓶頸：

- 若瓶頸只有 WAL fsync，TPS 應該大幅接近 target 600。
- 實際只從 330.66 提升到 371.57，約 12.4%。
- 所以目前瓶頸是「WAL fsync + Event Store append 寫入成本 + confirmed consumer 追趕 + projection lag」的組合。

不拆 DB 的下一步，不應繼續盲目增加 consumer。比較合理的方向是：

1. 降低 Order Event Store 單筆 append 寫入成本：檢查 index 數量、statement round trips、stream-head update 熱點。
2. 將 outbox relay / projection 的 DB 壓力進一步降級：更低頻、更大 batch、或只在主流程穩定時追趕。
3. 針對 PostgreSQL/WAL I/O 做正式調校：disk fsync latency、checkpoint、wal_buffers、commit_delay / group commit 行為。
4. 若仍要追穩態 600 TPS，才回到較大的架構選項：Order DB / Wallet DB 分離，或把 Order Event Store append path 做成更專用的 writer。

## 22. Scope Control

這次不做：

- Wallet 全面 Event Sourcing。
- Match Engine order book Event Sourcing。
- 跨服務 ACID transaction。
- 一開始就加入 snapshot。
- 每種 query 都建立 projection。

先完成 Order Aggregate、Event Store、Outbox 與單一 current-state projection，控制重構範圍。

## 23. 面試說法

> 我沒有把 Audit table 直接改名成 Event Store。Event Sourcing 的關鍵是 Event 成為唯一 source of truth、Aggregate 用歷史事件重建、append 使用 expected version 做 concurrency control，並由 projection 支援查詢。我把 Order 限定為第一個 Event-Sourced Aggregate：Event Store 保存 domain events，stream-head row 負責每個 order 的 version 與 hash，outbox 在同一 transaction 內建立 integration event，orders_current 則是可重建的 read model。Wallet 和 Match Engine 暫時維持現有交易與 Redis 模型，避免把整個系統一次重寫。
