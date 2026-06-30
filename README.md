# EAP Order Service

`eap-order` is the entry point for the trading flow.

It accepts order requests, maintains the order lifecycle, and reacts to downstream events from `eap-wallet` and `eap-matchEngine`.

## Responsibilities

- Expose REST APIs for buy/sell/auction order operations
- Publish order events into RabbitMQ
- Update order state from downstream confirmations, failures, and matches
- Provide order query and market-data endpoints

## What belongs here

- Order submission and cancellation
- Order state machine
- Order query / history
- Market data presentation

## What does not belong here

- Wallet balance locking logic
- Matching engine price-time execution
- AI orchestration / tool routing

## Main dependencies

- `eap-common` for shared DTOs and events
- RabbitMQ for async event flow
- PostgreSQL for order persistence

## Run

```bash
./gradlew :eap-order:bootRun
```

Default port: `8080`

## Notes

- This service is part of the core RabbitMQ-driven trading path.
- Keep request validation and state transitions explicit.
