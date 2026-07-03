# Audit Hash Chain Design

> 最後更新：2026-06-24  
> 範圍：Order / Auction lifecycle audit 的 correlation chain、初始事件與後續事件寫入規則。
> 後續 audit 的資料庫瓶頸、chain-head 模型、workload isolation 與容量驗證計畫，見 [`docs/audit-write-scaling-plan.md`](./audit-write-scaling-plan.md)。

## Chain 邊界

Audit chain 不是依 userId 建立。Order lifecycle 使用 `orderId` 作為 `correlationId`：

```text
order A: GENESIS -> ORDER_SUBMISSION_REQUESTED -> ORDER_CONFIRMED -> ORDER_MATCHED
order B: GENESIS -> ORDER_SUBMISSION_REQUESTED -> ORDER_FAILED
```

因此每張新訂單都有自己的 GENESIS；同一使用者的不同訂單不共用 hash chain。

## 原本問題

原本所有 audit event 都呼叫同一個 `record()`：

```text
SELECT latest audit FOR UPDATE
-> 查不到時使用 GENESIS
-> INSERT audit
```

對每個全新 orderId，第一個 SELECT 幾乎必然沒有結果。10 分鐘 soak test 中 Order Hikari pool 20/20 用滿、pending peak 109，而 Wallet DB pending 維持 0，這個同步 audit path 成為 Order 入口瓶頸。

更重要的是，舊順序先 publish MQ、再寫 `ORDER_SUBMITTED` audit。Wallet 很快回覆時，`ORDER_CONFIRMED` 有機會先成為 chain 起點，造成 lifecycle 語意反轉。

## Initial 與 Subsequent 分流

新訂單流程：

```text
backpressure check
-> allocate orderId / market sequence
-> recordInitial(ORDER_SUBMISSION_REQUESTED)
-> RabbitMQ publish + publisher confirm
-> Wallet processing
-> record(ORDER_CONFIRMED / ORDER_FAILED / ORDER_MATCHED)
```

`recordInitial()`：

- 固定使用 GENESIS，不查 previous hash。
- 使用 native `INSERT ... ON CONFLICT DO NOTHING`。
- 即使 MQ publish 失敗，`ORDER_SUBMISSION_REQUESTED` 仍如實表示使用者曾提出下單意圖。

`record()`：

- 只供後續 lifecycle event 使用。
- 查詢並鎖定相同 correlationId 的最新事件。
- 使用前一筆 hash 建立下一個 hash。

## DB 不變式與重試

PostgreSQL partial unique index：

```sql
CREATE UNIQUE INDEX uk_audit_events_single_genesis
ON order_service.audit_events(correlation_id)
WHERE prev_hash = repeat('0', 64);
```

同一個 orderId 被 client 重試時：

- initial audit insert 第二次為 no-op。
- RabbitMQ 仍可能收到 duplicate event。
- Wallet 使用 orderId idempotency claim，只處理一次。

真實 HTTP / PostgreSQL 驗證：

```text
same orderId HTTP requests       = 2 x 200
ORDER_SUBMISSION_REQUESTED rows  = 1
ORDER_CONFIRMED rows             = 1
GENESIS rows                     = 1
Wallet idempotency claims        = 1
```

另外以 100 筆新訂單驗證：wrong first event = 0、broken CONFIRMED link = 0、duplicate GENESIS = 0。

## 尚存限制

- Initial audit commit 與 RabbitMQ publish 不是同一個 atomic transaction；`REQUESTED` 可能存在但 publish 失敗。事件名稱刻意描述「提出意圖」，不宣稱 broker 已接受。
- Publisher confirm timeout 仍是 ambiguous failure，client 必須沿用相同 orderId 重試。
- 後續同一 correlationId 若有多個 event 真正同時寫入，僅鎖最新 audit row 的方式仍需要 concurrency stress test；production 化可考慮獨立 chain-head table 或 PostgreSQL advisory transaction lock。
- Audit table 是 append-only，大流量下需要 partition、retention 與 archive policy。
