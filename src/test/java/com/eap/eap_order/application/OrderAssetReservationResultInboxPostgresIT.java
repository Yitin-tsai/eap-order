package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationSucceededEvent;
import com.eap.common.event.OrderFailedEvent;
import com.eap.eap_order.configuration.observability.OrderDurableDebtSnapshotProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

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
    @Autowired
    private OrderDurableDebtSnapshotProvider durableDebt;

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

    @Test
    void durableDebt_shouldKeepAppliedIdentityConflictVisibleAsTerminal() {
        OrderAssetReservationSucceededEvent confirmed = confirmed();
        OrderFailedEvent failed = failed(confirmed.getOrderId(), confirmed.getUserId());
        inbox.receiveConfirmed(confirmed);
        OrderAssetReservationResultInbox.InboxEntry entry =
                inbox.claimRetryable(1, "worker-1", 30_000).get(0);
        assertThat(inbox.markApplied(entry, "worker-1")).isTrue();
        jdbc.update("""
                UPDATE order_service.order_asset_reservation_result_inbox
                SET received_at = CURRENT_TIMESTAMP - INTERVAL '2 minutes'
                WHERE order_id = ?
                """, confirmed.getOrderId());
        inbox.receiveFailed(failed);

        ReflectionTestUtils.invokeMethod(durableDebt, "refresh");
        var component = durableDebt.snapshot().components().stream()
                .filter(debt -> debt.work().equals("asset_reservation_result_inbox"))
                .findFirst()
                .orElseThrow();

        assertThat(component.totalCount()).isEqualTo(1);
        assertThat(component.retryCount()).isZero();
        assertThat(component.terminalCount()).isEqualTo(1);
        assertThat(component.oldestUnresolvedAgeSeconds()).isGreaterThanOrEqualTo(119);
    }

    @Test
    void durableDebt_shouldClassifyEveryOrderOwnedWorkFromAuthoritativeTables() {
        UUID assetOrder = UUID.randomUUID();
        UUID buyerOrder = UUID.randomUUID();
        UUID sellerOrder = UUID.randomUUID();
        UUID cancellationId = UUID.randomUUID();
        UUID cancelledOrder = UUID.randomUUID();
        UUID releaseCancellationId = UUID.randomUUID();
        UUID releaseEventId = UUID.randomUUID();
        UUID releasedOrder = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        UUID aggregateId = UUID.randomUUID();
        String tradeId = "REL103-" + UUID.randomUUID();
        try {
            jdbc.update("""
                    INSERT INTO order_service.order_asset_reservation_result_inbox
                        (order_id, result_type, payload, payload_hash, status, attempt_count,
                         error_type, received_at)
                    VALUES (?, 'CONFIRMED', '{}', 'hash', 'FAILED_RETRYABLE', 2,
                            'TRANSIENT_DATA_STORE', CURRENT_TIMESTAMP - INTERVAL '2 minutes')
                    """, assetOrder);
            jdbc.update("""
                    INSERT INTO order_service.order_trade_execution_inbox
                        (trade_id, buyer_order_id, seller_order_id, deal_price, quantity, payload,
                         status, attempt_count, received_at)
                    VALUES (?, ?, ?, 100, 1, '{}', 'PENDING_PREREQUISITE', 2,
                            CURRENT_TIMESTAMP - INTERVAL '2 minutes')
                    """, tradeId, buyerOrder, sellerOrder);
            jdbc.update("""
                    INSERT INTO order_service.order_cancellation_result_inbox
                        (cancellation_id, order_id, payload, status, attempt_count, received_at)
                    VALUES (?, ?, '{}', 'FAILED_PERMANENT', 1,
                            CURRENT_TIMESTAMP - INTERVAL '2 minutes')
                    """, cancellationId, cancelledOrder);
            jdbc.update("""
                    INSERT INTO order_service.order_asset_reservation_released_inbox
                        (cancellation_id, event_id, order_id, payload, payload_hash, status,
                         attempt_count, error_type, received_at)
                    VALUES (?, ?, ?, '{}', 'hash', 'FAILED_PERMANENT', 1,
                            'PERMANENT_INVARIANT', CURRENT_TIMESTAMP - INTERVAL '2 minutes')
                    """, releaseCancellationId, releaseEventId, releasedOrder);
            jdbc.update("""
                    INSERT INTO order_service.order_event_store
                        (event_id, aggregate_id, aggregate_type, aggregate_version, event_type,
                         payload_canonical, metadata_canonical, schema_version, occurred_at,
                         prev_hash, hash)
                    VALUES (?, ?, 'ORDER', 1, 'ProviderMatrixV1', '{}', '{}', 1,
                            CURRENT_TIMESTAMP - INTERVAL '2 minutes', repeat('0', 64), repeat('1', 64))
                    """, eventId, aggregateId);
            jdbc.update("""
                    INSERT INTO order_service.order_event_outbox
                        (event_id, aggregate_id, exchange_name, routing_key, payload, status,
                         attempt_count, message_type, created_at)
                    VALUES (?, ?, 'test.exchange', 'test.routing', '{}', 'PENDING', 3,
                            'java.lang.String', CURRENT_TIMESTAMP - INTERVAL '2 minutes')
                    """, eventId, aggregateId);

            ReflectionTestUtils.invokeMethod(durableDebt, "refresh");

            assertDebt("asset_reservation_result_inbox", 1, 1, 0);
            assertDebt("trade_execution_inbox", 1, 1, 0);
            assertDebt("cancellation_result_inbox", 1, 0, 1);
            assertDebt("asset_reservation_released_inbox", 1, 0, 1);
            assertDebt("event_outbox", 1, 1, 0);
            assertDebt("orders_current_projection", 1, 0, 0);
        } finally {
            jdbc.update("DELETE FROM order_service.order_event_outbox WHERE event_id = ?", eventId);
            jdbc.update("DELETE FROM order_service.order_event_store WHERE event_id = ?", eventId);
            jdbc.update("DELETE FROM order_service.order_asset_reservation_released_inbox WHERE event_id = ?",
                    releaseEventId);
            jdbc.update("DELETE FROM order_service.order_cancellation_result_inbox WHERE cancellation_id = ?",
                    cancellationId);
            jdbc.update("DELETE FROM order_service.order_trade_execution_inbox WHERE trade_id = ?", tradeId);
            jdbc.update("DELETE FROM order_service.order_asset_reservation_result_inbox WHERE order_id = ?",
                    assetOrder);
            ReflectionTestUtils.invokeMethod(durableDebt, "refresh");
        }
    }

    private void assertDebt(String work, long minimumTotal, long minimumRetry, long minimumTerminal) {
        var component = durableDebt.snapshot().components().stream()
                .filter(debt -> debt.work().equals(work))
                .findFirst()
                .orElseThrow();
        assertThat(component.totalCount()).isGreaterThanOrEqualTo(minimumTotal);
        assertThat(component.retryCount()).isGreaterThanOrEqualTo(minimumRetry);
        assertThat(component.terminalCount()).isGreaterThanOrEqualTo(minimumTerminal);
        assertThat(component.oldestUnresolvedAgeSeconds()).isGreaterThanOrEqualTo(119);
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
