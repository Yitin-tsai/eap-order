package com.eap.eap_order.domain.ordersourcing;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderCancellationAcceptedV1(
        UUID cancellationId,
        UUID orderId,
        UUID userId,
        int cancelledAmount,
        LocalDateTime acceptedAt) {
}
