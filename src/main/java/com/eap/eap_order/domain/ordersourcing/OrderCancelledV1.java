package com.eap.eap_order.domain.ordersourcing;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderCancelledV1(UUID orderId, UUID userId, LocalDateTime cancelledAt) {
}
