package com.eap.eap_order.eventstore;

import java.util.Objects;

public record OrderIntegrationEvent(
        String exchange,
        String routingKey,
        Object payload) {

    public OrderIntegrationEvent {
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(routingKey, "routingKey");
        Objects.requireNonNull(payload, "payload");
    }
}
