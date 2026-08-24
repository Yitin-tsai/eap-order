package com.eap.eap_order.domain.ordersourcing;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderCancellationRequestedV1(
        UUID cancellationId,
        UUID orderId,
        UUID userId,
        Integer originalAmount,
        LocalDateTime requestedAt) {
}
