# EAP Order Service

`eap-order` owns CDA order acceptance, the command-side order event stream, durable application of trade results, and rebuildable order query projections. It also exposes TDA bid entry and maintains auction status/result views.

## Current Flow

```text
POST /bid/buy or /bid/sell
  -> allocate market sequence
  -> append OrderSubmissionRequestedV1 + OrderSubmitted outbox atomically
  -> return PENDING_WALLET_CHECK

OrderAssetReservationSucceededEvent / OrderFailedEvent
  -> persist one unified asset-reservation-result inbox row per order_id
  -> ACK only after durable intake
  -> lease worker atomically appends the lifecycle event and marks the inbox APPLIED
  -> transient failures use backoff + jitter; conflicting results become durable incidents

TradeExecutedEvent
  -> apply immediately when both Order submission heads exist
  -> infer asset reservation SUCCEEDED when Trade arrives before confirmation
  -> persist to the durable trade inbox before ACK only when a prerequisite is genuinely missing
  -> retry technical failures and missing submission state from the inbox

cancel HTTP request
  -> append OrderCancellationRequestedV1 + outbox atomically with immutable original amount
  -> return 202 + cancellationId while MatchEngine arbitrates against matching
OrderCancellationResultEvent
  -> persist durable inbox before ACK
  -> append OrderCancellationAcceptedV1 only when earlier trade applications have caught up
  -> expose CANCELLING while Wallet releases the unmatched reservation
OrderAssetReservationReleasedEvent
  -> persist a second durable inbox before ACK
  -> append OrderCancellationCompletedV1 and expose CANCELLED

order event stream
  -> asynchronously rebuild orders_current projection
```

The query projection and Order's reservation-confirmation timing are not prerequisites for trade application. MatchEngine's durable `TradeExecuted` fact is sufficient authority to infer successful reservation and apply a valid trade to Order's command state. A late Wallet success still completes its durable inbox and lifecycle event, but cannot regress `PARTIALLY_MATCHED` or `MATCHED` back to `OPEN`.

Order stores the two state dimensions separately:

- `status`: Order execution lifecycle (`PENDING_ASSET_CHECK`, `OPEN`, `PARTIALLY_MATCHED`, `MATCHED`, `CANCELLING`, `CANCELLED`, `REJECTED`).
- `assetReservationStatus`: Wallet reservation progress (`PENDING`, `SUCCEEDED`, `REJECTED`, `RELEASED`).

The user-order query returns both fields and prefers the stronger command-side trade state when the rebuildable event projection is behind.

TDA follows a separate contract:

```text
auction bid HTTP
  -> validate and publish AuctionBidSubmittedEvent
  -> Wallet reserves the requested auction assets

AuctionCreatedEvent / AuctionClearedEvent
  -> persist the auction session or result view
  -> publish status/results to WebSocket clients
```

`AuctionBidSubmittedEvent` is currently published directly through RabbitMQ rather than the Order integration outbox. The auction-result listener also catches processing failures, so a failed local update does not automatically force broker redelivery. TDA is therefore implemented functionality, but it does not yet share the CDA path's publication, retry, or published capacity evidence.

## Ownership

| Owns | Does not own |
| --- | --- |
| CDA order IDs, market sequence allocation, order lifecycle events | Wallet balances and reservations |
| Order submission and integration-event outbox | Price-time matching decisions |
| Durable `TradeExecuted` inbox and `order_trade_applications` | The authoritative `TradeExecuted` fact |
| Durable cancellation intent, result inbox, release inbox, and final lifecycle | Cancellation arbitration and asset release calculation |
| Rebuildable current-order projection and audit-trail APIs | Cross-service completion state |
| TDA bid entry and auction status/result views | TDA asset reservation and auction clearing |

Order does not publish a per-trade completion callback to MatchEngine. Full-flow verification compares Order's durable trade applications with MatchEngine and Wallet outside the transaction path.

## Reliability

- Local state and outbox rows are committed atomically.
- RabbitMQ consumers ACK only after durable local handling.
- Wallet confirmation and rejection share one durable processing record, so the same order cannot silently accept contradictory terminal reservation results.
- A reservation rejection after an accepted trade is a permanent cross-service contradiction; it is isolated instead of overwriting the trade.
- The asset-reservation-result lease worker commits the Order lifecycle event and inbox `APPLIED` state in one consumer transaction; an expired lease is reclaimable after worker crash.
- `trade_id` and database constraints make repeated `TradeExecutedEvent` delivery idempotent.
- Projection-lagged events remain in a durable inbox and are reconciled without broker requeue storms.
- A cancellation result cannot override an earlier trade; it waits until the command-side remainder equals MatchEngine's cancelled remainder, then moves to `CANCELLING`.
- Final `CANCELLED` requires Wallet's durable `OrderAssetReservationReleasedEvent`; an early release fact remains a prerequisite retry instead of completing the order out of sequence.
- User-keyed order and cancellation rate limits plus queue-aware backpressure demonstrate bounded admission for the asynchronous pipeline. In this learning project, `userId` is the domain identity and an extension point for future quota policies; implementing a production authentication or gateway boundary is outside the current scope.
- TDA direct publication and caught listener failures are explicit gaps and are not described as transactional-outbox or retry protected.

## Current Performance Risk

The 2026-09-03 full-chain diagnostic found that RabbitMQ can drain while
`order_asset_reservation_result_inbox` continues to accumulate. A strict 15-minute
run passed at 200 orders/s, while 300 and 400 orders/s eventually converged but built
at least 51K/53K non-applied reservation-result rows. The load-test monitor now gates
this durable debt independently from broker backlog. The next optimization target is
the reservation-result worker/projector drain path; historical pre-inbox capacity does
not describe this revision.

## Run

```bash
./gradlew bootRun
```

Default port: `8080`; context path: `/eap-order`.

## Further Reading

- [Order event sourcing design](docs/order-event-sourcing-design.md)
- [MQ backpressure design](docs/mq-backpressure-design.md)
- [Order/Wallet admission load test](docs/order-wallet-e2e-load-test.md)
- [Audit write scaling plan](docs/audit-write-scaling-plan.md)
- [Wallet inbox and cancellation completion](https://github.com/Yitin-tsai/eap-infra/blob/main/docs/wallet-inbox-and-cancellation-completion.zh-TW.md)
- [2026-09-03 current-version full-chain diagnostic](https://github.com/Yitin-tsai/eap-infra/blob/main/docs/benchmarks/2026-09-03-current-version-full-chain.md)
- [EAP system architecture](https://github.com/Yitin-tsai/eap-infra/blob/main/docs/architecture.md)
