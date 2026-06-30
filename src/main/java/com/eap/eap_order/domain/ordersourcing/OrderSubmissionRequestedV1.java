package com.eap.eap_order.domain.ordersourcing;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderSubmissionRequestedV1(
        UUID orderId,
        UUID userId,
        String marketId,
        long marketSequence,
        String side,
        int price,
        int amount,
        LocalDateTime createdAt) {
}
