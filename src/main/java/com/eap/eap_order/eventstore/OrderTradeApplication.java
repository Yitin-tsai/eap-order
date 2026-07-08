package com.eap.eap_order.eventstore;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

public record OrderTradeApplication(
        String tradeId,
        UUID buyerOrderId,
        UUID sellerOrderId,
        Integer price,
        Integer quantity,
        LocalDateTime appliedAt) {

    public OrderTradeApplication {
        Objects.requireNonNull(tradeId, "tradeId");
        Objects.requireNonNull(buyerOrderId, "buyerOrderId");
        Objects.requireNonNull(sellerOrderId, "sellerOrderId");
        Objects.requireNonNull(price, "price");
        Objects.requireNonNull(quantity, "quantity");
        Objects.requireNonNull(appliedAt, "appliedAt");
        if (buyerOrderId.equals(sellerOrderId)) {
            throw new IllegalArgumentException("buyerOrderId and sellerOrderId must be different");
        }
    }
}
