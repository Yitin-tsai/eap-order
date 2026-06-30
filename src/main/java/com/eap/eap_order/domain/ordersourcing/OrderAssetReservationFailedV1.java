package com.eap.eap_order.domain.ordersourcing;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderAssetReservationFailedV1(
        UUID orderId,
        UUID userId,
        String reason,
        String failureType,
        LocalDateTime failedAt) {
}
