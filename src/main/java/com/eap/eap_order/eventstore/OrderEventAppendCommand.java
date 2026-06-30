package com.eap.eap_order.eventstore;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record OrderEventAppendCommand(
        UUID aggregateId,
        long expectedVersion,
        UUID eventId,
        String eventType,
        Object payload,
        Map<String, Object> metadata,
        int schemaVersion,
        LocalDateTime occurredAt,
        OrderIntegrationEvent integrationEvent) {

    public OrderEventAppendCommand {
        Objects.requireNonNull(aggregateId, "aggregateId");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(payload, "payload");
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        Objects.requireNonNull(occurredAt, "occurredAt");
        occurredAt = occurredAt.truncatedTo(ChronoUnit.MICROS);
        if (expectedVersion < 0) {
            throw new IllegalArgumentException("expectedVersion must be non-negative");
        }
        if (schemaVersion <= 0) {
            throw new IllegalArgumentException("schemaVersion must be positive");
        }
        if (eventType.isBlank()) {
            throw new IllegalArgumentException("eventType must not be blank");
        }
    }
}
