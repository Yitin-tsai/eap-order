# EAP Order Service

`eap-order` owns CDA order acceptance, the command-side order event stream, durable application of trade results, and rebuildable order query projections. It also exposes TDA bid entry and maintains auction status/result views.

## Current Flow

```text
POST /bid/buy or /bid/sell
  -> allocate market sequence
  -> append OrderSubmissionRequestedV1 + OrderSubmitted outbox atomically
  -> return PENDING_WALLET_CHECK

OrderConfirmedEvent / OrderFailedEvent
  -> append the corresponding order lifecycle event

TradeExecutedEvent
  -> persist durable inbox before manual ACK
  -> idempotently append command-side trade application events
  -> retry from the inbox when command state is not ready yet

cancel HTTP request
  -> append OrderCancellationRequestedV1 + outbox atomically with immutable original amount
  -> return 202 + cancellationId while MatchEngine arbitrates against matching
OrderCancellationResultEvent
  -> persist durable inbox before ACK
  -> apply CANCELLED only when earlier trade applications have caught up

order event stream
  -> asynchronously rebuild orders_current projection
```

The query projection is not a prerequisite for trade application. MatchEngine's durable `TradeExecuted` fact is sufficient authority to apply a valid trade to Order's command state; projection lag is measured and repaired separately.

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
| Durable cancellation intent and result inbox | Cancellation arbitration and asset release |
| Rebuildable current-order projection and audit-trail APIs | Cross-service completion state |
| TDA bid entry and auction status/result views | TDA asset reservation and auction clearing |

Order does not publish a per-trade completion callback to MatchEngine. Full-flow verification compares Order's durable trade applications with MatchEngine and Wallet outside the transaction path.

## Reliability

- Local state and outbox rows are committed atomically.
- RabbitMQ consumers ACK only after durable local handling.
- `trade_id` and database constraints make repeated `TradeExecutedEvent` delivery idempotent.
- Projection-lagged events remain in a durable inbox and are reconciled without broker requeue storms.
- A cancellation result cannot override an earlier trade; it waits until the command-side remainder equals MatchEngine's cancelled remainder.
- User-keyed order and cancellation rate limits plus queue-aware backpressure demonstrate bounded admission for the asynchronous pipeline. In this learning project, `userId` is the domain identity and an extension point for future quota policies; implementing a production authentication or gateway boundary is outside the current scope.
- TDA direct publication and caught listener failures are explicit gaps and are not described as transactional-outbox or retry protected.

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
- [EAP system architecture](https://github.com/Yitin-tsai/eap-infra/blob/main/docs/architecture.md)
