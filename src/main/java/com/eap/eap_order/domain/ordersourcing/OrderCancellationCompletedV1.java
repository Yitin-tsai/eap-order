package com.eap.eap_order.domain.ordersourcing;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderCancellationCompletedV1(
        UUID cancellationId,
        UUID orderId,
        UUID userId,
        int releasedQuantity,
        LocalDateTime completedAt) {
}
