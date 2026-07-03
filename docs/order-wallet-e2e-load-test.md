# Order → Wallet HTTP 全鏈路壓測

> 最後更新：2026-06-24  
> 範圍：從 Order HTTP API 到 RabbitMQ、Wallet transaction 與 Wallet Outbox Relay。

> 2026-06-25 註記：本文件第 2 節描述的是 Order Event Sourcing 前的同步 publisher-confirm 設計。Order 主寫入路徑已改為 `order_event_store` + `order_event_outbox`：HTTP request 先 durable append，RabbitMQ publish 由 relay 非同步完成。新的 Event Store 驗證結果記錄於 `docs/order-event-sourcing-design.md`。

## 1. 驗證目標

先前的 Wallet AMQP load test 是直接向 RabbitMQ 發送事件，能驗證 Wallet consumer、DB transaction、idempotency 與 outbox，但沒有經過 Order HTTP 入口。

本測試補齊下列正式路徑：

```text
HTTP Order API
  -> per-user rate limit
  -> wallet queue backpressure guard
  -> market sequence
  -> OrderEventPublisher
  -> RabbitMQ publisher confirm / mandatory return
  -> wallet.orderSubmitted.queue
  -> Wallet CreateOrderListener
  -> PostgreSQL transaction + idempotency
  -> Wallet Outbox
  -> RabbitMQ publisher confirm
```

## 2. Order 發布可靠性

Order 不導入完整 outbox，而是在同步接受訂單時等待 RabbitMQ correlated publisher confirm：

- broker ACK 且訊息可路由後，HTTP 才視為接受成功。
- NACK、mandatory return、timeout 或 publish exception 都回傳失敗，不會宣稱訂單已接受。
- backpressure 503 發生在 sequence 與 publish 前，仍由 client 依 `Retry-After` 重試。

這個選擇避免小規模先行版本增加 Order outbox poll delay 與維運複雜度，同時補上原本 `convertAndSend()` 無法證明 broker 已接受訊息的缺口。

限制：Order audit DB 與 MQ publish 仍不是同一個 atomic transaction。若未來要求「Order DB commit 與 event publish 絕不可分離」，才升級為 Order transactional outbox。

## 3. 測試工具與參數

新增 `OrderHttpLoadGenerator`，先透過 Wallet HTTP 建立測試使用者，再以 client-generated stable `orderId` 交錯送出 BUY/SELL request。它統計 HTTP 狀態、吞吐與 client-observed latency。

```bash
cd eap-order
GRADLE_USER_HOME=/Users/cfh00909120/Desktop/eap-workspace/.cache/gradle \
  ./gradlew --no-daemon orderHttpLoadTest \
  --args='--users 500 --events 10000 --tps 1000 --workers 128'
```

測試規模：

```text
users       = 500
orders      = 10,000
target TPS  = 1,000
workers     = 128
```

## 4. 第一輪：找出 Order 入口瓶頸

```text
accepted       = 10,000
HTTP 429       = 0
HTTP 503       = 0
other failures = 0
actual TPS     = 773.45
p50            = 112.53 ms
p95            = 312.73 ms
p99            = 1,519.75 ms
```

觀測結果：

```text
Order Hikari active peak  = 10
Order Hikari pending peak = 42
Wallet Hikari pending     = 0
Order publisher failed    = 0
Wallet outbox failed      = 0
```

瓶頸位於 Order：同步 audit / event processing 把預設 10 條 DB connection 用滿，而且逐筆 DEBUG/INFO log 放大 I/O 與 tail latency。Wallet DB pool 並未排隊，因此不應先增加 Wallet consumer 或 DB connection。

## 5. Load-test profile 調整

只調整 `application-loadtest.yml`：

```yaml
spring.datasource.hikari:
  maximum-pool-size: 20
  minimum-idle: 5

logging.level:
  '[com.eap.eap_order]': WARN
```

設計原則：

- production profile 不跟著壓測數字盲目調大。
- 連線池大小必須由 active/pending 指標決定，不由 worker 數直接決定。
- 壓測關閉逐筆業務 log，但錯誤仍保留；吞吐量不能主要量到 console I/O。

## 6. 第二輪結果

2026-06-24 使用相同資料量與速率重跑：

```text
accepted       = 10,000
HTTP 429       = 0
HTTP 503       = 0
other failures = 0
elapsed        = 10.02 s
actual TPS     = 998.18
p50            = 51.28 ms
p95            = 148.50 ms
p99            = 474.10 ms
```

可靠性與最終狀態：

```text
Order publisher confirmed = 10,000
Order publisher failed    = 0
Wallet outbox published   = 10,000
Wallet outbox failed      = 0
Wallet queue ready        = 0
Wallet queue unacked      = 0
Outbox pending            = 0
Outbox failed             = 0
Duplicate order IDs       = 0
Negative wallets          = 0
```

壓測期間峰值（Prometheus 取樣）：

```text
Order Hikari active peak  = 14 / 20
Order Hikari pending peak = 4
Wallet Hikari active peak = 6
Wallet Hikari pending     = 0
Wallet queue depth peak   = 4,994
Wallet outbox pending peak= 6,736
Order process CPU peak    ~= 11.9%
Wallet process CPU peak   ~= 4.5%
```

## 7. 結論

- 目前小規模先行版本能在本機完成約 1,000 HTTP orders/s，且 10,000 筆請求沒有遺失、拒絕或 publish failure。
- Order 原本的限制是 DB pool contention 與逐筆 logging，不是 RabbitMQ 或 Wallet DB。
- Queue 與 outbox 在尖峰期間會累積，但測後可清空；這是正常削峰，不代表能無限制承載。此輪 queue peak 4,994 已接近 5,000 warning threshold。
- 本結果是單機、本機網路、10 秒 workload 的容量證據，不等於 production SLA。下一步應做較長時間 soak test 與 publisher failure injection，而不是再盲目提高 TPS。

## 8. Publisher Failure Injection

2026-06-24 補上 deterministic failure injection，覆蓋：

| 注入情境 | 預期結果 |
|----------|----------|
| RabbitMQ ACK | 計入 confirmed，正常接受 |
| RabbitMQ NACK | 拒絕接受，計入 failed |
| ACK 但 mandatory return / `NO_ROUTE` | 拒絕接受，計入 failed |
| confirm future 超時 | 拒絕接受，計入 failed |
| `convertAndSend()` 同步 exception | 拒絕接受，計入 failed |

所有失敗路徑都必須：

- 丟出 `OrderPublishException`。
- 不增加 confirmed metric。
- 增加一次 failed metric。
- 記錄 confirm duration。
- 經 REST exception handler 回 `503 Service Unavailable`、`Retry-After: 5` 與 `ORDER_PUBLISH_UNAVAILABLE`。

這裡使用 publisher 邊界的 deterministic injection，而不是依賴手動關閉 RabbitMQ。原因是 NACK、return 與 timeout 是不同協定結果；只關閉 broker 只能穩定產生 connection exception，不能證明其他分支正確。

## 9. Wallet Consumer Concurrency Experiment

為降低 4-consumer 基準的 queue depth peak 4,994，曾在相同單機環境將 Wallet consumer base concurrency 從 4 提高到 6，其他壓測參數不變。因 `max-concurrency` 仍為 8，持續負載期間 Spring listener 實際自動擴到 7 個 consumer。

```text
                              base 4         base 6 (observed 7)
actual TPS                    998.18         998.14
p50                           51.28 ms       96.12 ms
p95                           148.50 ms      193.29 ms
p99                           474.10 ms      416.27 ms
wallet queue depth peak       4,994          5,679
wallet outbox pending peak    6,736          9,035
wallet Hikari active peak     6              8
wallet Hikari pending peak    0              0
order Hikari active peak      14             20
order Hikari pending peak     4              33
```

結果不保留 6-consumer 設定，load-test profile 還原為 4。增加 consumer 沒有提高端到端吞吐，也沒有降低 backlog，反而讓 Order latency、共享 PostgreSQL contention 與 outbox backlog 惡化。

這個實驗說明 consumer concurrency 不是免費的水平擴充：目前 Order 與 Wallet 使用同一個 PostgreSQL instance，增加 Wallet transaction concurrency 會與 Order audit transaction 競爭 DB 資源。下一步應先降低每筆 Wallet transaction 的 DB round trips，並分別觀察 claim、wallet lock/update 與 outbox insert latency；若仍需擴充，再評估資料庫資源隔離，而不是繼續增加 consumer thread。

## 10. Wallet Transaction 與 Outbox Relay 優化

先加入固定低基數的 transaction stage timers，4-consumer 基準平均值：

```text
wallet transaction total = 6.43 ms
idempotency claim         = 0.74 ms
wallet lookup             = 0.95 ms
outbox serialization/save = 1.45 ms
```

曾將成功路徑的 `SELECT wallet + UPDATE wallet` 改成一條 conditional atomic UPDATE。真實壓測中 transaction 平均反而成為 6.90 ms，queue peak 沒有下降，因此未保留這個版本。少一條 SQL 不代表一定更快；PostgreSQL 仍需取得 row lock，而且整體瓶頸包含 outbox 與共享 DB contention。

最後保留的優化位於 Outbox Relay：

```text
舊：publish 1 -> wait confirm 1 -> save SENT 1 -> 下一筆

新：依 created_at 順序 publish 最多 200 筆
    -> 以共同 deadline 等待各筆 confirm
    -> 一條 bulk UPDATE 將成功項目標成 SENT
```

可靠性語意不變：

- 訊息仍依查詢順序送入 RabbitMQ。
- 只有 ACK 且沒有 mandatory return 的事件才能進入成功集合。
- NACK、return、timeout 仍逐筆記錄 retry metadata。
- 整批 confirm 共用單一 timeout deadline，避免 200 筆各等 5 秒。
- broker ACK 後、bulk UPDATE 前 crash 會造成重送而不是遺失；依既有 idempotent consumer 處理。

最終相同 workload、每 500 ms 直接取樣 RabbitMQ：

```text
accepted                = 10,000
actual TPS              = 992.64
p50                     = 58.27 ms
p95                     = 120.02 ms
p99                     = 219.22 ms
wallet queue depth peak = 3,913
queue final             = 0
outbox pending final    = 0
outbox failed final     = 0
duplicate order IDs     = 0
negative wallets        = 0
```

相較原先觀察到的 queue peak 4,994，本輪下降約 22%，且 API latency 沒有被犧牲。短測試容易被 Prometheus scrape interval 漏採樣，因此此數字使用 500 ms RabbitMQ Management API sampling；100 ms sampling 曾明顯干擾 broker，該輪結果已排除。

## 11. 10-minute Soak Test

2026-06-24 執行第一輪長時間測試：

```text
duration    = 600 seconds
offered TPS= 1,000
attempts    = 600,000
users       = 500
workers     = 128
```

Generator 改為 bounded in-flight submission，避免一次建立 600,000 個等待中的 task；每 30 秒輸出 accepted / 429 / 503 / failure progress。

結果：

```text
accepted       = 371,083
HTTP 429       = 16,841
HTTP 503       = 212,076
other failures = 0
sustained TPS  = 618.41
p50            = 61.46 ms
p95            = 206.54 ms
p99            = 495.73 ms
```

容量與穩定性：

```text
wallet queue depth peak       = 10,721
wallet outbox pending peak    = 2,641
oldest pending outbox peak    = 2 seconds
Order Hikari active peak      = 20 / 20
Order Hikari pending peak     = 109
Wallet Hikari active peak     = 9 / 10
Wallet Hikari pending peak    = 0
Order CPU peak                ~= 14.3%
Wallet CPU peak               ~= 6.6%
Order heap observed range     = 2-192 MiB
Wallet heap observed range    = 0-68 MiB
```

測後狀態：

```text
Order publisher confirmed = 371,083
Order publisher failed    = 0
Wallet consumed           = 371,083
Wallet outbox published   = 371,083
Queue final               = 0
Outbox pending / failed   = 0 / 0
Duplicate order IDs       = 0
Negative wallets          = 0
```

結論：短時間 1,000 TPS 可以吸收，但不可持續 10 分鐘。Backpressure 正確把 queue 保護在 hard threshold 附近，沒有 event loss 或資料錯誤；真正的持續容量約為 620 accepted orders/s。

主要瓶頸是 Order DB，而不是 Wallet 或 outbox：Order pool 打滿且有 109 個 pending thread，但 CPU 仍很低。`AuditService.record()` 對每個全新 orderId 先執行 `findLatestByCorrelationIdForUpdate()`，結果必然不存在，再 INSERT 第一筆 audit。下一個優化目標應是 initial audit 的直接 GENESIS insert，保留後續 event 的 hash-chain lookup。

資料成長也需要列入 production 設計：測後 `audit_events` 約 474 MiB、wallet outbox 約 205 MiB、idempotency table 約 70 MiB（包含先前測試資料）。需規劃 audit partition/retention、SENT outbox cleanup 與 idempotency retention，否則長期容量會受儲存與索引成長影響。

本輪 429 部分來自 generator 在 in-flight 阻塞後追趕舊排程形成 burst；後續已改為落後時從當下重新排程、不補送 burst。這不影響大量 503 所揭露的 Order DB / queue 持續容量限制，但下一輪比較應使用修正後 pacing。

## 12. Initial Audit Optimization

Soak test 後將第一筆 audit 與後續 audit 分流。`ORDER_SUBMISSION_REQUESTED` 在 MQ publish 前用 GENESIS direct insert；後續 CONFIRMED / FAILED / MATCHED 才查 previous hash。詳細設計見 [`docs/audit-chain-design.md`](./audit-chain-design.md)。

在 audit table 已成長到約 94 萬列後，以修正過、不追趕 burst 的 bounded generator 測得：

```text
events / accepted = 10,000 / 10,000
actual TPS        = 721.60
p50               = 169.26 ms
p95               = 301.13 ms
p99               = 613.79 ms
wallet queue peak = 8 (Prometheus sample)
```

這不能和早期會追趕排程的 998 TPS 短測直接比較；較有意義的對照是先前長測約 618 accepted TPS。Initial direct insert 減少一個 DB lookup，但 Order Hikari 仍有 20 active / 110 pending，因為每張 accepted order 還會由 CONFIRMED listener 執行後續 audit lookup + insert。優化已修正 lifecycle ordering 並降低初始事件成本，但沒有完全解除同步 audit DB 瓶頸。
