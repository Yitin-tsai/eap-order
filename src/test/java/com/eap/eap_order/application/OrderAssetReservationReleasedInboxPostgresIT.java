package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationReleasedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.datasource.url=${EAP_ORDER_POSTGRES_IT_URL:jdbc:postgresql://localhost:5432/eapdb}",
                "spring.datasource.username=${EAP_ORDER_POSTGRES_IT_USER:admin}",
                "spring.datasource.password=${EAP_ORDER_POSTGRES_IT_PASSWORD:admin123}",
                "spring.jpa.hibernate.ddl-auto=validate",
                "spring.liquibase.enabled=true",
                "spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.xml",
                "spring.rabbitmq.listener.simple.auto-startup=false",
                "eap.scheduling.enabled=false",
                "eap.wallet.base-url=http://localhost:8081/eap-wallet",
                "eap.matchEngine.base-url=http://localhost:8082/match-engine",
                "eap.order.trade-execution-reconciler.initial-delay-ms=3600000",
                "eap.order.trade-execution-reconciler.poll-interval-ms=3600000"
        })
@EnabledIfSystemProperty(named = "eap.integration.postgres", matches = "true")
class OrderAssetReservationReleasedInboxPostgresIT {

    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private OrderAssetReservationReleasedInbox inbox;
    private UUID cancellationId;

    @BeforeEach
    void setUp() {
        cancellationId = UUID.randomUUID();
    }

    @AfterEach
    void cleanup() {
        jdbc.update("""
                DELETE FROM order_service.order_asset_reservation_released_inbox
                WHERE cancellation_id = ?
                """, cancellationId);
    }

    @Test
    void identicalDuplicate_shouldBeIgnored_butChangedPayloadShouldBeRecordedAsConflict() {
        OrderAssetReservationReleasedEvent event = event(5);

        assertThat(inbox.receive(event)).isEqualTo(OrderAssetReservationReleasedInbox.ReceiveOutcome.ACCEPTED);
        assertThat(inbox.receive(event)).isEqualTo(OrderAssetReservationReleasedInbox.ReceiveOutcome.DUPLICATE);
        assertThat(inbox.receive(event(6))).isEqualTo(OrderAssetReservationReleasedInbox.ReceiveOutcome.CONFLICT);

        assertThat(status()).isEqualTo("FAILED_PERMANENT");
        assertThat(jdbc.queryForObject("""
                SELECT COUNT(*) FROM order_service.order_asset_reservation_released_inbox
                WHERE cancellation_id = ? AND conflict_detected_at IS NOT NULL
                """, Integer.class, cancellationId)).isEqualTo(1);
    }

    @Test
    void expiredLease_shouldBeReclaimed() {
        inbox.receive(event(5));
        assertThat(inbox.claimRetryable(1, "worker-a", 30_000)).hasSize(1);
        jdbc.update("""
                UPDATE order_service.order_asset_reservation_released_inbox
                SET claim_until = CURRENT_TIMESTAMP - INTERVAL '1 second'
                WHERE cancellation_id = ?
                """, cancellationId);

        List<OrderAssetReservationReleasedInbox.InboxEntry> reclaimed =
                inbox.claimRetryable(1, "worker-b", 30_000);

        assertThat(reclaimed).hasSize(1);
        assertThat(reclaimed.get(0).attemptCount()).isEqualTo(2);
    }

    private OrderAssetReservationReleasedEvent event(int quantity) {
        UUID orderId = UUID.nameUUIDFromBytes((cancellationId + ":order").getBytes());
        return OrderAssetReservationReleasedEvent.builder()
                .eventId(UUID.nameUUIDFromBytes((cancellationId + ":event").getBytes()))
                .cancellationId(cancellationId)
                .orderId(orderId)
                .userId(UUID.nameUUIDFromBytes((cancellationId + ":user").getBytes()))
                .orderType("BUY")
                .releasedQuantity(quantity)
                .releasedAt(LocalDateTime.of(2026, 9, 1, 12, 0))
                .build();
    }

    private String status() {
        return jdbc.queryForObject("""
                SELECT status FROM order_service.order_asset_reservation_released_inbox
                WHERE cancellation_id = ?
                """, String.class, cancellationId);
    }
}
