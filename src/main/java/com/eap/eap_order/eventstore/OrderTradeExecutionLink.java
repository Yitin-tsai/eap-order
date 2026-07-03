package com.eap.eap_order.eventstore;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

public record OrderTradeExecutionLink(
        String tradeId,
        UUID orderId,
        String side,
        Integer price,
        Integer quantity,
        LocalDateTime appliedAt) {

    public OrderTradeExecutionLink {
        Objects.requireNonNull(tradeId, "tradeId");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(side, "side");
        Objects.requireNonNull(price, "price");
        Objects.requireNonNull(quantity, "quantity");
        Objects.requireNonNull(appliedAt, "appliedAt");
    }
}
