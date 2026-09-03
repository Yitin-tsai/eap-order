package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationReleasedEvent;
import com.eap.common.event.OrderCancellationResultEvent;
import com.eap.common.event.OrderAssetReservationSucceededEvent;
import com.eap.common.event.OrderSubmittedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
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
                "eap.matchEngine.base-url=http://localhost:8082/match-engine"
        })
@EnabledIfSystemProperty(named = "eap.integration.postgres", matches = "true")
class OrderCancellationCompletionPostgresIT {

    @Autowired OrderEventSourcingService service;
    @Autowired OrderAssetReservationReleasedInbox inbox;
    @Autowired OrderAssetReservationReleasedProcessor processor;
    @Autowired JdbcTemplate jdbc;

    private UUID orderId;

    @AfterEach
    void cleanup() {
        if (orderId == null) {
            return;
        }
        jdbc.update("DELETE FROM order_service.order_asset_reservation_released_inbox WHERE order_id = ?", orderId);
        jdbc.update("DELETE FROM order_service.order_cancellation_result_inbox WHERE order_id = ?", orderId);
        jdbc.update("DELETE FROM order_service.order_asset_reservation_result_inbox WHERE order_id = ?", orderId);
        jdbc.update("DELETE FROM order_service.order_event_outbox WHERE aggregate_id = ?", orderId);
        jdbc.update("DELETE FROM order_service.order_event_store WHERE aggregate_id = ?", orderId);
        jdbc.update("DELETE FROM order_service.orders_current WHERE order_id = ?", orderId);
        jdbc.update("DELETE FROM order_service.order_matching_state WHERE order_id = ?", orderId);
        jdbc.update("DELETE FROM order_service.order_stream_heads WHERE aggregate_id = ?", orderId);
    }

    @Test
    void cancellation_shouldBecomeFinalOnlyAfterWalletReleaseFact() {
        Fixture fixture = fixture();

        assertThat(streamStatus()).isEqualTo("CANCELLING");
        inbox.receive(fixture.released());
        OrderAssetReservationReleasedInbox.InboxEntry entry =
                inbox.claimRetryable(1, "release-worker", 30_000).get(0);

        processor.process(entry, "release-worker");

        assertThat(streamStatus()).isEqualTo("CANCELLED");
        assertThat(eventCount("OrderCancellationAcceptedV1")).isEqualTo(1);
        assertThat(eventCount("OrderCancellationCompletedV1")).isEqualTo(1);
        assertThat(inboxStatus(fixture.cancellationId())).isEqualTo("APPLIED");
    }

    @Test
    void lostLease_shouldRollbackFinalDomainEventAndLeaveOrderCancelling() {
        Fixture fixture = fixture();
        inbox.receive(fixture.released());
        OrderAssetReservationReleasedInbox.InboxEntry entry =
                inbox.claimRetryable(1, "real-owner", 30_000).get(0);

        assertThatThrownBy(() -> processor.process(entry, "wrong-owner"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Lost asset-release inbox lease");

        assertThat(streamStatus()).isEqualTo("CANCELLING");
        assertThat(eventCount("OrderCancellationCompletedV1")).isZero();
        assertThat(inboxStatus(fixture.cancellationId())).isEqualTo("IN_PROGRESS");
    }

    private Fixture fixture() {
        orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        LocalDateTime now = LocalDateTime.now();
        OrderSubmittedEvent submitted = OrderSubmittedEvent.builder()
                .orderId(orderId).userId(userId).marketId("ENERGY-SPOT").marketSequence(1L)
                .orderType("BUY").price(100).amount(5).createdAt(now).build();
        service.request(submitted);
        service.confirm(OrderAssetReservationSucceededEvent.builder()
                .orderId(orderId).userId(userId).marketId("ENERGY-SPOT").marketSequence(1L)
                .orderType("BUY").price(100).amount(5).createdAt(now).build());
        UUID cancellationId = service.requestCancellation(orderId, userId);
        service.applyCancellationResult(OrderCancellationResultEvent.builder()
                .cancellationId(cancellationId).orderId(orderId).userId(userId)
                .outcome(OrderCancellationResultEvent.CANCELLED)
                .orderType("BUY").limitPrice(100).cancelledAmount(5).decidedAt(now).build());
        OrderAssetReservationReleasedEvent released = OrderAssetReservationReleasedEvent.builder()
                .eventId(UUID.nameUUIDFromBytes(("release:" + cancellationId).getBytes()))
                .cancellationId(cancellationId).orderId(orderId).userId(userId)
                .orderType("BUY").releasedQuantity(5).releasedAt(now).build();
        return new Fixture(cancellationId, released);
    }

    private String streamStatus() {
        return jdbc.queryForObject("""
                SELECT status FROM order_service.order_stream_heads WHERE aggregate_id = ?
                """, String.class, orderId);
    }

    private int eventCount(String eventType) {
        return jdbc.queryForObject("""
                SELECT COUNT(*) FROM order_service.order_event_store
                WHERE aggregate_id = ? AND event_type = ?
                """, Integer.class, orderId, eventType);
    }

    private String inboxStatus(UUID cancellationId) {
        return jdbc.queryForObject("""
                SELECT status FROM order_service.order_asset_reservation_released_inbox
                WHERE cancellation_id = ?
                """, String.class, cancellationId);
    }

    private record Fixture(UUID cancellationId, OrderAssetReservationReleasedEvent released) {
    }
}
