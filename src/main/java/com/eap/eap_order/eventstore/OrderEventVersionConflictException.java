package com.eap.eap_order.eventstore;

import java.util.UUID;

public class OrderEventVersionConflictException extends RuntimeException {

    public OrderEventVersionConflictException(UUID aggregateId, long expectedVersion, long actualVersion) {
        super("Order event stream version conflict: aggregateId=" + aggregateId
                + ", expectedVersion=" + expectedVersion
                + ", actualVersion=" + actualVersion);
    }
}
