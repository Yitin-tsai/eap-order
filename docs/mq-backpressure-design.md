# MQ Backpressure Design

> 最後更新：2026-06-22  
> 範圍：`eap-order` 在接受新訂單前，依 wallet queue backlog 與 consumer 狀態進行 admission control。
> Order 接受後的 RabbitMQ publisher confirm 與完整 HTTP 壓測，見 [`docs/order-wallet-e2e-load-test.md`](./order-wallet-e2e-load-test.md)。

## 1. 問題背景

RabbitMQ 可以吸收短暫流量尖峰，但不是無限 buffer。如果 order publish rate 長期高於 wallet consume rate，queue depth 會持續成長，造成：

- 訂單等待 wallet 驗資的時間持續增加。
- 記憶體、磁碟與 broker recovery 成本上升。
- API 仍回成功，但系統實際已無法在合理時間完成訂單。
- 下游完全停止時，上游仍持續接受流量，放大故障。

因此不能只監控 backlog；必須在新訂單進入 MQ 前執行 admission control。

## 2. 設計邊界

Guard 放在 `PlaceBuyOrderService` 與 `PlaceSellOrderService` 的最前面：

```text
REST / MCP order request
  -> wallet queue backpressure guard
  -> allocate market sequence
  -> build OrderSubmittedEvent
  -> publish RabbitMQ
  -> audit
```

這個位置確保：

- REST 與 MCP 下單共用同一條規則。
- 被拒絕的訂單不會消耗 market sequence。
- 被拒絕的訂單不會發布 MQ event 或留下成功 audit。

Auction bid 使用不同流程與 queue，不在這次範圍。

## 3. Queue Probe 與快取

Guard 使用 Spring AMQP `AmqpAdmin.getQueueInfo()` 取得：

```text
wallet.orderSubmitted.queue message count
wallet.orderSubmitted.queue consumer count
```

Queue probe 結果快取 1 秒。所有並發請求共用 snapshot，快取過期時只有一個 thread 重新查詢 RabbitMQ。

設計理由：

- 不在每個 HTTP request 上增加一次 broker round trip。
- 允許 admission decision 有最多 1 秒的短暫落差。
- Queue 狀態屬於容量訊號，不需要逐 request 強一致。

## 4. Admission Policy

預設設定：

```yaml
eap:
  backpressure:
    wallet-queue:
      enabled: true
      hard-threshold: 10000
      cache-ttl-ms: 1000
      hard-retry-after-seconds: 5
```

決策表：

| 狀態 | HTTP | 行為 |
|------|------|------|
| depth < 10,000 且 consumers > 0 | 正常流程 | 接受訂單；depth ≥ 5,000 時只告警 |
| depth ≥ 10,000 | `503 Service Unavailable` | 拒絕新訂單，`Retry-After: 5` |
| consumer count = 0 | `503 Service Unavailable` | Fail closed |
| queue 不存在或 probe exception | `503 Service Unavailable` | Fail closed |

### 429 與 503 的界線

既有 per-user rate limit 在單一使用者送太快時回 `429 Too Many Requests`。Queue backlog 是整個 Wallet downstream 的容量問題，因此只使用 `503 Service Unavailable`，避免兩種不同問題共用 429。5,000 是告警門檻，不拒絕訂單；10,000 才啟動 admission rejection。

## 5. Error Contract

範例：

```http
HTTP/1.1 503 Service Unavailable
Retry-After: 5
Content-Type: application/json
```

```json
{
  "error": "ORDER_BACKPRESSURE",
  "level": "UNAVAILABLE",
  "message": "Wallet order queue is unavailable or has no active consumer",
  "queueDepth": 0,
  "retryAfterSeconds": 5
}
```

Level：

- `HARD`
- `UNAVAILABLE`

收到 503 後，Order Service 不會自行重試。這筆請求在 event 建立、market sequence、audit 與 MQ publish 前就已結束，因此也不會寫入任何 Order Outbox。Client 可依 `Retry-After` 重新提交；若 Client 不重試，這筆下單意圖就不會進入系統。

即使未來為正常訂單加入 Order Outbox，被 backpressure 拒絕的請求仍不應寫入 outbox，否則只是把 backlog 從 RabbitMQ 搬到 PostgreSQL，沒有真正執行 admission control。

## 6. Metrics

`eap-order` 新增 Actuator 與 Prometheus registry，暴露：

| Metric | 類型 | 意義 |
|--------|------|------|
| `eap_order_wallet_queue_depth` | Gauge | 最近一次快取的 queue depth；未知為 -1 |
| `eap_order_wallet_queue_consumers` | Gauge | 最近一次 consumer count；未知為 -1 |
| `eap_order_backpressure_hard_rejected_total` | Counter | Hard/unavailable 503 次數 |
| `eap_order_backpressure_probe_failed_total` | Counter | Queue 不存在或 RabbitMQ probe 失敗次數 |

Prometheus scrape path：

```text
/eap-order/actuator/prometheus
```

## 7. Dashboard 與 Alerts

Grafana dashboard：`EAP Order Backpressure`

- Wallet Order Queue Depth
- Wallet Queue Consumers
- Order Backpressure Rejections
- RabbitMQ Queue Probe Failures

Prometheus rules：

- Queue consumer 持續為 0。
- Queue depth 持續高於 8,000，接近 hard limit。
- 五分鐘內發生 hard rejection。
- 五分鐘內發生 queue probe failure。

設定檔：

- `observability/grafana/dashboards/eap-order.json`
- `observability/prometheus/rules/eap-order-backpressure.yml`

## 8. 驗證結果

單元測試涵蓋：

- 健康 queue 接受訂單。
- 1 秒 cache 內不重複 probe。
- Hard threshold 拒絕並映射 503。
- Consumer 為 0 時 fail closed。
- Queue 不存在時 fail closed 並記錄 probe failure。
- Guard 關閉時不存取 RabbitMQ。

真實環境驗證：

```text
wallet consumer count = 0
POST /bid/buy          = HTTP 503
Retry-After            = 5
hard rejected total    = 1
market sequence        = not allocated
```

Wallet 啟動後：

```text
wallet consumer count = 4
queue depth           = 0
POST /bid/buy         = HTTP 200
order event           = published
```

Prometheus 已成功 scrape `eap-order` 與 `eap-wallet` targets。

## 9. 設計取捨與限制

- Queue depth 目前來自 AMQP queue-declare 的 message count，主要反映 ready messages，不是完整 end-to-end latency。
- 1 秒 cache 代表極端突發下可能多接受少量訂單；這是降低 per-request broker overhead 的取捨。
- Consumer count > 0 不代表 consumer 一定健康，因此仍需搭配 queue growth、processing latency 與 DB metrics。
- 5,000 告警門檻與 10,000 拒絕門檻是第一版保守值，不是 production capacity 結論；應依 drain rate、允許等待時間與壓測調整。
- Guard 使用單一 queue 的全域狀態，尚未提供 per-market rate limit。
- Client retry 必須加入 jitter，且不得無限立即重試，否則會形成 retry storm。

## 10. 設計理念

1. **MQ absorbs bursts, not permanent overload**：允許短暫 backlog，但拒絕無限累積。
2. **Reject before side effects**：在 sequence、publish 與 audit 前決定是否接受。
3. **Fail closed for trading admission**：無法確認 wallet 能承接時，不宣稱訂單已成功提交。
4. **Cached capacity signal**：使用短期快取降低 admission control 本身的成本。
5. **Explicit client contract**：個別使用者超速使用既有 429；下游容量問題使用 503 與 `Retry-After`。
