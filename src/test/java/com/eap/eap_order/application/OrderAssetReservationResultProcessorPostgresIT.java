package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationSucceededEvent;
import com.eap.common.event.OrderSubmittedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.datasource.url=jdbc:postgresql://localhost:5432/eapdb",
                "spring.datasource.username=admin",
                "spring.datasource.password=admin123",
                "spring.jpa.hibernate.ddl-auto=validate",
                "spring.liquibase.enabled=true",
                "spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.xml",
                "spring.rabbitmq.listener.simple.auto-startup=false",
                "eap.scheduling.enabled=false",
                "eap.wallet.base-url=http://localhost:8081/eap-wallet",
                "eap.matchEngine.base-url=http://localhost:8082/match-engine",
                "eap.order.asset-reservation-result-reconciler.initial-delay-ms=3600000",
                "eap.order.asset-reservation-result-reconciler.poll-interval-ms=3600000"
        })
@EnabledIfSystemProperty(named = "eap.integration.postgres", matches = "true")
class OrderAssetReservationResultProcessorPostgresIT {

    @Autowired
    private OrderEventSourcingService service;
    @Autowired
    private OrderAssetReservationResultInbox inbox;
    @Autowired
    private OrderAssetReservationResultProcessor processor;
    @Autowired
    private JdbcTemplate jdbc;

    private final List<UUID> orderIds = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        for (UUID orderId : orderIds) {
            jdbc.update("DELETE FROM order_service.order_asset_reservation_result_inbox WHERE order_id = ?", orderId);
            jdbc.update("DELETE FROM order_service.order_event_outbox WHERE aggregate_id = ?", orderId);
            jdbc.update("DELETE FROM order_service.order_event_store WHERE aggregate_id = ?", orderId);
            jdbc.update("DELETE FROM order_service.orders_current WHERE order_id = ?", orderId);
            jdbc.update("DELETE FROM order_service.order_matching_state WHERE order_id = ?", orderId);
            jdbc.update("DELETE FROM order_service.order_stream_heads WHERE aggregate_id = ?", orderId);
        }
    }

    @Test
    void process_shouldCommitDomainEventAndInboxAppliedTogether() {
        Fixture fixture = fixture();
        OrderAssetReservationResultInbox.InboxEntry entry =
                inbox.claimRetryable(1, "worker-1", 30_000).get(0);

        processor.process(entry, "worker-1");

        assertThat(eventCount(fixture.orderId(), "OrderAssetReservationConfirmedV1")).isEqualTo(1);
        assertThat(inboxStatus(fixture.orderId())).isEqualTo("APPLIED");
    }

    @Test
    void process_whenLeaseIsLost_shouldRollbackDomainEvent() {
        Fixture fixture = fixture();
        OrderAssetReservationResultInbox.InboxEntry entry =
                inbox.claimRetryable(1, "worker-1", 30_000).get(0);

        assertThatThrownBy(() -> processor.process(entry, "wrong-worker"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Lost asset-reservation-result inbox lease");

        assertThat(eventCount(fixture.orderId(), "OrderAssetReservationConfirmedV1")).isZero();
        assertThat(inboxStatus(fixture.orderId())).isEqualTo("IN_PROGRESS");
    }

    private Fixture fixture() {
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        orderIds.add(orderId);
        LocalDateTime createdAt = LocalDateTime.of(2026, 8, 31, 10, 0);
        OrderSubmittedEvent submitted = OrderSubmittedEvent.builder()
                .orderId(orderId)
                .userId(userId)
                .marketId("ENERGY-SPOT")
                .marketSequence(1L)
                .price(100)
                .amount(3)
                .orderType("BUY")
                .createdAt(createdAt)
                .build();
        service.request(submitted);
        OrderAssetReservationSucceededEvent confirmed = OrderAssetReservationSucceededEvent.builder()
                .orderId(orderId)
                .userId(userId)
                .marketId("ENERGY-SPOT")
                .marketSequence(1L)
                .price(100)
                .amount(3)
                .orderType("BUY")
                .createdAt(createdAt)
                .build();
        inbox.receiveConfirmed(confirmed);
        return new Fixture(orderId);
    }

    private long eventCount(UUID orderId, String eventType) {
        Long result = jdbc.queryForObject("""
                SELECT COUNT(*)
                FROM order_service.order_event_store
                WHERE aggregate_id = ? AND event_type = ?
                """, Long.class, orderId, eventType);
        return result == null ? 0 : result;
    }

    private String inboxStatus(UUID orderId) {
        return jdbc.queryForObject("""
                SELECT status
                FROM order_service.order_asset_reservation_result_inbox
                WHERE order_id = ?
                """, String.class, orderId);
    }

    private record Fixture(UUID orderId) {
    }
}
