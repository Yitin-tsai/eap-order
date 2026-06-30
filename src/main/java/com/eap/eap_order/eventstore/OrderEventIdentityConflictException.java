package com.eap.eap_order.eventstore;

import java.util.UUID;

public class OrderEventIdentityConflictException extends RuntimeException {

    public OrderEventIdentityConflictException(UUID eventId) {
        super("eventId was reused with different event content: eventId=" + eventId);
    }
}
