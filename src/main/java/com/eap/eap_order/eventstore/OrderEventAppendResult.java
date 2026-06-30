package com.eap.eap_order.eventstore;

import java.util.UUID;

public record OrderEventAppendResult(
        UUID aggregateId,
        UUID eventId,
        long aggregateVersion,
        long globalPosition,
        String hash,
        boolean duplicate) {
}
