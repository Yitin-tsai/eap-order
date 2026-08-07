# Audit Write Scaling Plan

> **已完成遷移的歷史計畫。** 本文的瓶頸數據與 Phase 1/切換規劃描述 2026-06 當時版本。現行 BUY/SELL 路徑已切換到 order event store + integration outbox；`orders_current` 為非同步可重建投影，`TradeExecutedEvent` 由 durable inbox 套用。現況請以 [Order README](../README.md) 與 [Order Event Sourcing 設計](./order-event-sourcing-design.md) 開頭的 current boundary 為準。
>
> 最後更新：2026-06-24  
> 目標：解除 Order DB connection pool 飽和，同時保留 audit hash chain、冪等與事件順序。
> Order Service Event Sourcing 的目標架構、Event Store、Aggregate 與 Projection 設計，見 [`docs/order-event-sourcing-design.md`](./order-event-sourcing-design.md)。
> 當時更新：Order Event Sourcing Phase 1 schema 與 atomic append primitive 已完成；該文件撰寫時尚未切換 BUY/SELL write path，現行版本已完成切換。

## 1. 問題不是單純「SQL 太慢」

10 分鐘、目標 1,000 TPS 的 soak test 結果：

```text
accepted orders              = 371,083
sustained accepted TPS       = 618.41
HTTP 503 backpressure        = 212,076
Order Hikari active peak     = 20 / 20
Order Hikari pending peak    = 109
Wallet Hikari pending peak   = 0
Order CPU peak               ~= 14.3%
Wallet queue depth peak      = 10,721
```

這是典型的 database wait bottleneck：CPU 很低，但所有 Order connections 被 transaction 占用，新 request 排隊等待 connection。增加 CPU 或 Wallet consumer 無法解決。

每個 accepted order 至少產生：

```text
ORDER_SUBMISSION_REQUESTED initial audit : 1 INSERT
ORDER_CONFIRMED / ORDER_FAILED audit      : latest lookup + 1 INSERT
後續 MATCHED / CANCELLED                  : latest lookup + 1 INSERT
```

以 620 orders/s 且每張訂單先產生兩筆 audit 估算，Audit 已接近 1,240 events/s。20 條 connection 在飽和狀態下，平均每個 audit transaction 只要持有 connection 約 16 ms，就會用完整個 pool：

```text
required concurrency ~= throughput × connection hold time
20 ~= 1,240 × 0.016 seconds
```

因此真正問題是：audit write amplification、同步 connection 占用，以及 audit workload 與 Order request 共用 pool。

## 2. Current Query 與 Storage Evidence

目前 `audit_events` 約 942k rows：

```text
heap size  = 491 MiB
index size = 247 MiB
total size = 738 MiB
```

已知 correlationId 的 latest query：

```sql
SELECT *
FROM order_service.audit_events
WHERE correlation_id = ?
ORDER BY id DESC
LIMIT 1
FOR UPDATE;
```

Warm-cache execution 約 0.31 ms。這表示「查 previous hash」不是唯一瓶頸；刪除這一條 query 不足以消除 pool saturation。每次 INSERT 還要更新多個索引並寫入 JSONB heap/WAL。

Index evidence：

| Index | Size | Observed scans | 說明 |
|-------|------|----------------|------|
| `uk_audit_events_hash` | 112 MiB | 0 | 全域 hash 唯一索引；每次 INSERT 都維護 |
| `idx_audit_correlation_id` | 45 MiB | 932k+ | latest-chain 查詢必要 |
| `idx_audit_created_at` | 36 MiB | 0 | 未來 retention/時間查詢可能需要 |
| `uk_audit_events_single_genesis` | 26 MiB | 10k+ | GENESIS 唯一性必要 |
| Primary key | 20 MiB | 0 | row identity 必要 |
| `idx_audit_event_type` | 8 MiB | 1 | 目前 workload 幾乎不用 |

`hash` 的全域唯一性不是 hash-chain 正確性的必要條件；chain validation 只需要每列 hash 可重算、`prev_hash` 能連到前一事件。SHA-256 collision 本身已極低，112 MiB unique index 主要增加 write amplification。是否移除仍需先確認沒有管理查詢依賴 hash lookup。

## 3. Correctness Gap：鎖 Tail Row 不等於鎖 Chain

目前後續 writer 查詢 latest row 並 `FOR UPDATE`。若兩個後續事件同時看到同一個 tail，它們可能都用相同 `prev_hash` 建立新事件，形成 fork。當 chain 尚無 row 時，`SELECT ... FOR UPDATE` 更無法鎖住「不存在的資料」。

Initial audit 已透過 partial unique index 解決多 GENESIS，但後續 chain append 仍需要明確的 per-correlation serialization point。

此外 RabbitMQ 是 at-least-once delivery；目前 audit event 沒有獨立 `event_id` unique constraint，duplicate business event 可能被寫成兩筆合法但重複的 audit。

所以效能優化不能只做 asynchronous `@Async`。若沒有 chain lock 與 event idempotency，只會更快地製造 fork 或 duplicate。

## 4. 解法比較

| 解法 | 能否解除 request pool 壓力 | Chain 正確性 | 複雜度 | 判斷 |
|------|---------------------------|--------------|--------|------|
| 直接增加 Hikari pool | 低；可能把壓力轉給 PostgreSQL | 不改善 | 低 | 不建議作為主解 |
| 移除不必要索引 | 中等，降低每次 INSERT 成本 | 不影響 | 低 | 第一階段 |
| `(correlation_id, id DESC)` index | 小幅降低 latest lookup/sort | 不改善 fork | 低 | 可做但不是終局 |
| PostgreSQL advisory lock | 不降低 DB round trips | 可序列化 chain | 中 | 過渡方案 |
| `audit_chain_head` table | latest lookup 不再掃大表 | 提供明確 row lock | 中 | 推薦核心模型 |
| DB stored function append | 一次 client round trip | 可在 DB transaction 內保證 | 中高、綁 PostgreSQL | 本專案適合 |
| 單純 `@Async` | 只隱藏 request latency | 可能遺失、亂序 | 低 | 不接受 |
| Durable audit queue + writer | 高，可隔離 request path | 需 partition/idempotency | 高 | Production 演進 |
| Audit 專用 DB | 高隔離 | 需處理一致性 | 高 | 流量再上升時 |
| Table partition/retention | 改善長期 storage/index | 不直接解短期 pool | 中高 | 必要長期工作 |

## 5. 推薦模型：Chain Head + Idempotent Append

新增小型 head table：

```sql
CREATE TABLE order_service.audit_chain_heads (
    correlation_id varchar(50) PRIMARY KEY,
    last_hash      varchar(64) NOT NULL,
    last_sequence  bigint NOT NULL,
    updated_at     timestamp NOT NULL
);
```

Audit event 增加：

```text
event_id       UUID / source message ID, UNIQUE
chain_sequence BIGINT, UNIQUE(correlation_id, chain_sequence)
```

Append transaction：

```text
1. 以 event_id 判斷 duplicate；duplicate 直接成功返回
2. SELECT chain head FOR UPDATE
3. 驗證 source sequence / lifecycle transition
4. 使用 head.last_hash 計算新 hash
5. INSERT audit event
6. UPDATE chain head 的 hash / sequence
7. COMMIT
```

優點：

- 鎖的是固定 head row，不是持續膨脹的 audit tail。
- 同 correlationId 強制序列化；不同訂單仍可平行。
- Latest hash 查詢成本不隨 audit history 成長。
- `event_id` 接住 RabbitMQ duplicate delivery。
- Audit history 可以按時間 partition/archive，因為寫入不需要跨 partition 找 latest row。

### 為何考慮 PostgreSQL function

若全部由 Java/JPA 執行，append 至少需要 head SELECT、audit INSERT、head UPDATE 三次 statement。可以用 PostgreSQL function 將它們包成一次 client round trip：

```text
append_audit_event(event_id, correlation_id, event_type, user_id, payload, occurred_at)
-> lock head
-> deduplicate
-> compute / insert / update
-> return hash + sequence
```

這會增加 PostgreSQL 綁定，但 EAP 已使用 PostgreSQL-specific JSONB、partial index 與 locking；對學習型專案而言，這個 tradeoff 可以清楚展示「用 DB function 縮短 connection hold / network round trips」。

Hash canonicalization 必須只有一個真相來源。如果改由 DB function 計算，Java `verifyChain()` 也要使用完全相同的 payload canonical form、timestamp precision 與欄位拼接規則，否則會產生無法驗證的 hash。

## 6. Workload Isolation：不要讓 Audit 餓死 Order API

即使單次 append 變快，Audit 與 Order request 共用同一 Hikari pool 仍缺少 bulkhead。

演進順序：

1. 先建立 audit 專用 connection pool，設定較小上限；避免 confirmed/matched audit consumer 把 request pool 20 條全部拿走。
2. 後續 lifecycle audit 改由 durable RabbitMQ audit queue 處理，不使用不可靠的 in-memory `@Async`。
3. Writer 可以跨不同 correlationId 做 JDBC batch；同 correlationId 仍依 sequence 排序。
4. 流量再增加時，將 Audit Writer 與資料庫獨立部署。

需要理解：separate pool 是資源隔離，不會憑空增加 PostgreSQL IOPS。若兩個 pool 仍指向同一 DB instance，總容量瓶頸仍可能存在，但至少 API admission、publisher confirm 不會被 audit consumer connection starvation 拖垮。

## 7. Index 與 Retention 計畫

短期先用 `pg_stat_user_indexes` 與實際 query inventory 決定：

- 評估移除 `uk_audit_events_hash`，改由 validation job 檢查 chain，不維護全域唯一 B-tree。
- 評估移除或延後建立極少使用的 `event_type` index。
- 將 correlation index 改成 `(correlation_id, id DESC)`；chain head 上線後，history query可改成 `(correlation_id, chain_sequence)`。

長期：

- `audit_events` 依 `created_at` 做月/日 partition。
- 熱資料保留在主庫，舊 partition archive 到 object storage / cold DB。
- Audit 不應直接 delete 單列，否則破壞 chain；archive 單位必須保留可驗證的 boundary hash。
- Outbox SENT 與 idempotency table 也需要各自 retention，不與 audit retention 混用。

## 8. 建議實作階段

### Phase 1：Measurement + Low-risk Write Reduction

- 新增 initial/subsequent audit latency、failure、duplicate metrics。
- 建立 SQL/query inventory。
- 移除確認不需要的 write-heavy index。
- 加入後續 audit concurrent append stress test，先證明現況是否會 fork。

### Phase 2：Correct Append Primitive

- 新增 `event_id`、`chain_sequence`、`audit_chain_heads`。
- 實作一次 round-trip 的 append function。
- 將 initial 與 subsequent 都轉到同一個 idempotent append primitive；initial sequence = 0。
- 驗證 duplicate、out-of-order、concurrent append、rollback。

### Phase 3：Bulkhead / Async Writer

- Audit 專用 pool。
- 後續 audit 走 durable queue，不使用裸 `@Async`。
- 跨 correlation JDBC batch，保留 per-correlation ordering。

### Phase 4：Capacity Proof

使用修正後、不追趕 burst 的 generator：

```text
600 TPS × 10 minutes
700 TPS × 10 minutes
800 TPS × 10 minutes
必要時再測 1,000 TPS
```

驗收條件：

```text
HTTP unexpected failure       = 0
Order Hikari pending peak     = 0（或短暫且可回落）
wallet queue sustained growth = 0
outbox oldest pending         < 30 seconds
duplicate audit event_id      = 0
duplicate chain sequence      = 0
chain fork                    = 0
queue/outbox final            = 0
```

## 9. 面試回答重點

> 我先用 soak test 發現 CPU 很低，但 Order connection pool 20 條全滿、pending thread 超過 100，因此不是算力不足，而是同步 audit transaction 造成 DB wait。第一步不是盲目加 pool，而是分析每個 order 的 write amplification、索引成本與 connection hold time。Initial audit 已改成 GENESIS direct insert；後續則規劃用 correlation chain-head row 作為 serialization point，加 event-id idempotency，並用 PostgreSQL function 將 lock、insert、head update 包成一次 round trip。再用獨立 pool 或 durable audit writer 做 bulkhead，避免 audit consumer 餓死 Order API。最後以固定 TPS soak test 驗證 pool pending、queue growth、chain fork 與資料 retention，而不是只看短測 TPS。
