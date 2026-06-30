package com.eap.eap_order.domain.ordersourcing;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderMatchedV1(
        UUID orderId,
        int matchId,
        int amount,
        int dealPrice,
        LocalDateTime matchedAt) {
}
