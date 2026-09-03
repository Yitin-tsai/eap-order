package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationSucceededEvent;
import com.eap.common.event.OrderFailedEvent;
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
class OrderAssetReservationResultInboxPostgresIT {

    @Autowired
    private OrderAssetReservationResultInbox inbox;
    @Autowired
    private JdbcTemplate jdbc;

    private final List<UUID> orderIds = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        for (UUID orderId : orderIds) {
            jdbc.update("""
                    DELETE FROM order_service.order_asset_reservation_result_inbox
                    WHERE order_id = ?
                    """, orderId);
        }
    }

    @Test
    void duplicateConfirmed_shouldKeepOnePendingRecord() {
        OrderAssetReservationSucceededEvent event = confirmed();

        assertThat(inbox.receiveConfirmed(event))
                .isEqualTo(OrderAssetReservationResultInbox.ReceiveOutcome.ACCEPTED);
        assertThat(inbox.receiveConfirmed(event))
                .isEqualTo(OrderAssetReservationResultInbox.ReceiveOutcome.DUPLICATE);

        assertThat(row(event.getOrderId()).status()).isEqualTo("PENDING");
        assertThat(count(event.getOrderId())).isEqualTo(1);
    }

    @Test
    void conflictingResultBeforeApplication_shouldBecomePermanentDebt() {
        OrderAssetReservationSucceededEvent confirmed = confirmed();
        OrderFailedEvent failed = failed(confirmed.getOrderId(), confirmed.getUserId());

        inbox.receiveConfirmed(confirmed);

        assertThat(inbox.receiveFailed(failed))
                .isEqualTo(OrderAssetReservationResultInbox.ReceiveOutcome.CONFLICT);

        InboxRow row = row(confirmed.getOrderId());
        assertThat(row.status()).isEqualTo("FAILED_PERMANENT");
        assertThat(row.errorType()).isEqualTo("IDENTITY_CONFLICT");
        assertThat(row.conflictingResultType()).isEqualTo("FAILED");
        assertThat(inbox.retryPermanentFailure(confirmed.getOrderId())).isFalse();
    }

    @Test
    void conflictingResultAfterApplication_shouldRecordIncidentWithoutUndoingAppliedFact() {
        OrderAssetReservationSucceededEvent confirmed = confirmed();
        OrderFailedEvent failed = failed(confirmed.getOrderId(), confirmed.getUserId());
        inbox.receiveConfirmed(confirmed);
        OrderAssetReservationResultInbox.InboxEntry entry =
                inbox.claimRetryable(1, "worker-1", 30_000).get(0);
        assertThat(inbox.markApplied(entry, "worker-1")).isTrue();

        assertThat(inbox.receiveFailed(failed))
                .isEqualTo(OrderAssetReservationResultInbox.ReceiveOutcome.CONFLICT);

        InboxRow row = row(confirmed.getOrderId());
        assertThat(row.status()).isEqualTo("APPLIED");
        assertThat(row.errorType()).isEqualTo("IDENTITY_CONFLICT");
        assertThat(row.conflictingResultType()).isEqualTo("FAILED");
    }

    @Test
    void expiredLease_shouldBeClaimedByAnotherWorker() {
        OrderAssetReservationSucceededEvent event = confirmed();
        inbox.receiveConfirmed(event);
        assertThat(inbox.claimRetryable(1, "crashed-worker", 30_000)).hasSize(1);
        jdbc.update("""
                UPDATE order_service.order_asset_reservation_result_inbox
                SET claim_until = CURRENT_TIMESTAMP - INTERVAL '1 second'
                WHERE order_id = ?
                """, event.getOrderId());

        List<OrderAssetReservationResultInbox.InboxEntry> reclaimed =
                inbox.claimRetryable(1, "recovery-worker", 30_000);

        assertThat(reclaimed).hasSize(1);
        assertThat(reclaimed.get(0).orderId()).isEqualTo(event.getOrderId());
        assertThat(reclaimed.get(0).attemptCount()).isEqualTo(2);
    }

    @Test
    void technicalPermanentFailure_shouldRequireExplicitRetry() {
        OrderAssetReservationSucceededEvent event = confirmed();
        inbox.receiveConfirmed(event);
        jdbc.update("""
                UPDATE order_service.order_asset_reservation_result_inbox
                SET status = 'FAILED_PERMANENT', error_type = 'RETRY_EXHAUSTED_TRANSIENT_DATABASE'
                WHERE order_id = ?
                """, event.getOrderId());

        assertThat(inbox.retryPermanentFailure(event.getOrderId())).isTrue();
        assertThat(row(event.getOrderId()).status()).isEqualTo("FAILED_RETRYABLE");
    }

    private OrderAssetReservationSucceededEvent confirmed() {
        UUID orderId = UUID.randomUUID();
        orderIds.add(orderId);
        return OrderAssetReservationSucceededEvent.builder()
                .orderId(orderId)
                .userId(UUID.randomUUID())
                .marketId("ENERGY-SPOT")
                .marketSequence(1L)
                .price(100)
                .amount(3)
                .orderType("BUY")
                .createdAt(LocalDateTime.of(2026, 8, 31, 10, 0))
                .build();
    }

    private OrderFailedEvent failed(UUID orderId, UUID userId) {
        return OrderFailedEvent.builder()
                .orderId(orderId)
                .userId(userId)
                .reason("餘額不足")
                .failureType("INSUFFICIENT_BALANCE")
                .failedAt(LocalDateTime.of(2026, 8, 31, 10, 1))
                .build();
    }

    private long count(UUID orderId) {
        Long result = jdbc.queryForObject("""
                SELECT COUNT(*)
                FROM order_service.order_asset_reservation_result_inbox
                WHERE order_id = ?
                """, Long.class, orderId);
        return result == null ? 0 : result;
    }

    private InboxRow row(UUID orderId) {
        return jdbc.queryForObject("""
                SELECT status, error_type, conflicting_result_type
                FROM order_service.order_asset_reservation_result_inbox
                WHERE order_id = ?
                """, (rs, rowNum) -> new InboxRow(
                rs.getString("status"),
                rs.getString("error_type"),
                rs.getString("conflicting_result_type")), orderId);
    }

    private record InboxRow(String status, String errorType, String conflictingResultType) {
    }
}
