package com.eap.eap_order.domain.ordersourcing;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderAggregateTest {

    @Test
    void lifecycle_shouldDeriveStateFromEvents() {
        OrderAggregate order = new OrderAggregate();
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();

        order.request(orderId, userId, "ENERGY-TWD", 1, "BUY", 10, 10, LocalDateTime.now());
        order.confirmAssetReservation(LocalDateTime.now());
        order.match(1, 4, 10, LocalDateTime.now());

        assertEquals(OrderLifecycleStatus.PARTIALLY_MATCHED, order.status());
        assertEquals(4, order.matchedAmount());
        assertEquals(6, order.remainingAmount());
        assertEquals(3, order.version());

        order.match(2, 6, 10, LocalDateTime.now());

        assertEquals(OrderLifecycleStatus.MATCHED, order.status());
        assertEquals(0, order.remainingAmount());
        assertEquals(4, order.version());
    }

    @Test
    void illegalTransition_shouldBeRejectedWithoutNewVersion() {
        OrderAggregate order = new OrderAggregate();
        order.request(UUID.randomUUID(), UUID.randomUUID(), "ENERGY-TWD", 1,
                "SELL", 10, 5, LocalDateTime.now());

        assertThrows(IllegalStateException.class,
                () -> order.match(1, 5, 10, LocalDateTime.now()));
        assertEquals(1, order.version());
        assertEquals(OrderLifecycleStatus.PENDING_ASSET_CHECK, order.status());
    }
}
