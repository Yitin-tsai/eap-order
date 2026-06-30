package com.eap.eap_order.domain.ordersourcing;

import java.time.LocalDateTime;
import java.util.UUID;

public class OrderAggregate {

    private UUID orderId;
    private UUID userId;
    private String marketId;
    private long marketSequence;
    private String side;
    private int price;
    private int originalAmount;
    private int matchedAmount;
    private OrderLifecycleStatus status = OrderLifecycleStatus.NOT_CREATED;
    private long version;

    public OrderSubmissionRequestedV1 request(
            UUID orderId,
            UUID userId,
            String marketId,
            long marketSequence,
            String side,
            int price,
            int amount,
            LocalDateTime createdAt) {
        if (status != OrderLifecycleStatus.NOT_CREATED) {
            throw new IllegalStateException("Order already exists: " + orderId);
        }
        if (price <= 0 || amount <= 0) {
            throw new IllegalArgumentException("Order price and amount must be positive");
        }
        OrderSubmissionRequestedV1 event = new OrderSubmissionRequestedV1(
                orderId, userId, marketId, marketSequence, side, price, amount, createdAt);
        apply(event);
        return event;
    }

    public OrderAssetReservationConfirmedV1 confirmAssetReservation(LocalDateTime confirmedAt) {
        requireStatus(OrderLifecycleStatus.PENDING_ASSET_CHECK, "confirm");
        OrderAssetReservationConfirmedV1 event = new OrderAssetReservationConfirmedV1(orderId, userId, confirmedAt);
        apply(event);
        return event;
    }

    public OrderAssetReservationFailedV1 failAssetReservation(String reason, String failureType, LocalDateTime failedAt) {
        requireStatus(OrderLifecycleStatus.PENDING_ASSET_CHECK, "fail");
        OrderAssetReservationFailedV1 event = new OrderAssetReservationFailedV1(orderId, userId, reason, failureType, failedAt);
        apply(event);
        return event;
    }

    public OrderMatchedV1 match(int matchId, int amount, int dealPrice, LocalDateTime matchedAt) {
        if (status != OrderLifecycleStatus.OPEN && status != OrderLifecycleStatus.PARTIALLY_MATCHED) {
            throw new IllegalStateException("Cannot match order in status " + status);
        }
        if (amount <= 0 || matchedAmount + amount > originalAmount) {
            throw new IllegalArgumentException("Invalid matched amount: " + amount);
        }
        OrderMatchedV1 event = new OrderMatchedV1(orderId, matchId, amount, dealPrice, matchedAt);
        apply(event);
        return event;
    }

    public OrderCancelledV1 cancel(UUID actorUserId, LocalDateTime cancelledAt) {
        if (status != OrderLifecycleStatus.OPEN && status != OrderLifecycleStatus.PARTIALLY_MATCHED) {
            throw new IllegalStateException("Cannot cancel order in status " + status);
        }
        if (!userId.equals(actorUserId)) {
            throw new IllegalArgumentException("Only the order owner can cancel this order");
        }
        OrderCancelledV1 event = new OrderCancelledV1(orderId, actorUserId, cancelledAt);
        apply(event);
        return event;
    }

    public void apply(Object event) {
        if (event instanceof OrderSubmissionRequestedV1 requested) {
            orderId = requested.orderId();
            userId = requested.userId();
            marketId = requested.marketId();
            marketSequence = requested.marketSequence();
            side = requested.side();
            price = requested.price();
            originalAmount = requested.amount();
            matchedAmount = 0;
            status = OrderLifecycleStatus.PENDING_ASSET_CHECK;
        } else if (event instanceof OrderAssetReservationConfirmedV1) {
            status = OrderLifecycleStatus.OPEN;
        } else if (event instanceof OrderAssetReservationFailedV1) {
            status = OrderLifecycleStatus.REJECTED;
        } else if (event instanceof OrderMatchedV1 matched) {
            matchedAmount += matched.amount();
            status = matchedAmount == originalAmount
                    ? OrderLifecycleStatus.MATCHED
                    : OrderLifecycleStatus.PARTIALLY_MATCHED;
        } else if (event instanceof OrderCancelledV1) {
            status = OrderLifecycleStatus.CANCELLED;
        } else {
            throw new IllegalArgumentException("Unsupported Order domain event: " + event.getClass().getName());
        }
        version++;
    }

    private void requireStatus(OrderLifecycleStatus required, String action) {
        if (status != required) {
            throw new IllegalStateException("Cannot " + action + " order in status " + status);
        }
    }

    public UUID orderId() { return orderId; }
    public UUID userId() { return userId; }
    public String marketId() { return marketId; }
    public long marketSequence() { return marketSequence; }
    public String side() { return side; }
    public int price() { return price; }
    public int originalAmount() { return originalAmount; }
    public int matchedAmount() { return matchedAmount; }
    public int remainingAmount() { return originalAmount - matchedAmount; }
    public OrderLifecycleStatus status() { return status; }
    public long version() { return version; }
}
