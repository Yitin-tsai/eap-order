package com.eap.eap_order.eventstore;

import com.eap.eap_order.domain.ordersourcing.OrderCancelledV1;
import com.eap.eap_order.domain.ordersourcing.OrderMatchedV1;
import com.eap.eap_order.domain.ordersourcing.OrderSubmissionRequestedV1;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;

@Component
public class OrdersCurrentProjector {

    private static final String PROJECTION_NAME = "orders_current";

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final int batchSize;
    private final boolean enabled;
    private final int maxBatchesPerTick;
    private final int repairBatchSize;
    private final boolean repairEnabled;

    public OrdersCurrentProjector(
            @Qualifier("orderProjectionJdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            @Qualifier("orderProjectionTransactionManager") PlatformTransactionManager transactionManager,
            @Value("${eap.order-projection.batch-size:500}") int batchSize,
            @Value("${eap.order-projection.enabled:true}") boolean enabled,
            @Value("${eap.order-projection.max-batches-per-tick:1}") int maxBatchesPerTick,
            @Value("${eap.order-projection.repair.batch-size:100}") int repairBatchSize,
            @Value("${eap.order-projection.repair.enabled:true}") boolean repairEnabled) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.batchSize = batchSize;
        this.enabled = enabled;
        this.maxBatchesPerTick = Math.max(1, maxBatchesPerTick);
        this.repairBatchSize = repairBatchSize;
        this.repairEnabled = repairEnabled;
    }

    @Scheduled(fixedDelayString = "${eap.order-projection.poll-interval-ms:100}")
    public void project() {
        if (!enabled) {
            return;
        }
        projectBatches(maxBatchesPerTick);
    }

    public void projectUntilCaughtUp() {
        if (!enabled) {
            return;
        }
        projectUntilCaughtUpIgnoringEnabled();
    }

    public void projectUntilCaughtUpIgnoringEnabled() {
        projectBatches(Integer.MAX_VALUE);
    }

    private void projectBatches(int maxBatches) {
        boolean fullBatch;
        int batches = 0;
        do {
            Boolean result = transactionTemplate.execute(status -> projectBatch());
            fullBatch = Boolean.TRUE.equals(result);
            batches++;
        } while (fullBatch && batches < maxBatches);
    }

    private boolean projectBatch() {
        jdbc.update("""
                INSERT INTO order_service.projection_checkpoints
                    (projection_name, last_global_position, updated_at)
                VALUES (?, 0, CURRENT_TIMESTAMP)
                ON CONFLICT (projection_name) DO NOTHING
                """, PROJECTION_NAME);
        Long checkpoint = jdbc.queryForObject("""
                SELECT last_global_position
                FROM order_service.projection_checkpoints
                WHERE projection_name = ?
                FOR UPDATE
                """, Long.class, PROJECTION_NAME);
        List<ProjectionEvent> events = jdbc.query("""
                SELECT global_position, aggregate_id, aggregate_version,
                       event_type, payload_canonical
                FROM order_service.order_event_store
                WHERE global_position > ?
                ORDER BY global_position
                LIMIT ?
                """, (rs, rowNum) -> new ProjectionEvent(
                rs.getLong("global_position"),
                rs.getObject("aggregate_id", UUID.class),
                rs.getLong("aggregate_version"),
                rs.getString("event_type"),
                rs.getString("payload_canonical")), checkpoint, batchSize);

        long lastPosition = checkpoint == null ? 0 : checkpoint;
        for (ProjectionEvent event : events) {
            apply(event);
            lastPosition = event.globalPosition();
        }
        if (!events.isEmpty()) {
            jdbc.update("""
                    UPDATE order_service.projection_checkpoints
                    SET last_global_position = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE projection_name = ?
                    """, lastPosition, PROJECTION_NAME);
        }
        return events.size() == batchSize;
    }

    @Scheduled(fixedDelayString = "${eap.order-projection.repair.poll-interval-ms:60000}")
    public void repair() {
        if (!enabled || !repairEnabled) {
            return;
        }
        transactionTemplate.executeWithoutResult(status -> repairStaleProjections());
    }

    private void apply(ProjectionEvent event) {
        try {
            switch (event.eventType()) {
                case "OrderSubmissionRequestedV1" -> applyRequested(
                        event, objectMapper.readValue(event.payload(), OrderSubmissionRequestedV1.class));
                case "OrderAssetReservationConfirmedV1" -> applyAssetReservationConfirmed(event);
                case "OrderAssetReservationFailedV1" -> applyAssetReservationFailed(event);
                case "OrderMatchedV1" -> applyMatched(
                        event, objectMapper.readValue(event.payload(), OrderMatchedV1.class));
                case "OrderCancellationRequestedV1" -> advanceVersion(event);
                case "OrderCancellationAcceptedV1" -> updateStatus(event, "CANCELLING");
                case "OrderCancellationCompletedV1" -> applyCancellationCompleted(event);
                case "OrderCancelledV1" -> applyCancellationCompleted(event);
                default -> throw new IllegalArgumentException("Unsupported projection event: " + event.eventType());
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Projection failed at globalPosition=" + event.globalPosition(), e);
        }
    }

    private void applyRequested(ProjectionEvent event, OrderSubmissionRequestedV1 requested) {
        int inserted = jdbc.update("""
                INSERT INTO order_service.orders_current
                    (order_id, user_id, market_id, market_sequence, side, price,
                     original_amount, remaining_amount, matched_amount, status, asset_reservation_status,
                     aggregate_version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'PENDING_ASSET_CHECK', 'PENDING', ?, ?, ?)
                ON CONFLICT (order_id) DO NOTHING
                """, requested.orderId(), requested.userId(), requested.marketId(),
                requested.marketSequence(), requested.side(), requested.price(),
                requested.amount(), requested.amount(), event.aggregateVersion(),
                requested.createdAt(), requested.createdAt());
        ensureAppliedOrAlreadyProjected(event, inserted);
    }

    private void updateStatus(ProjectionEvent event, String status) {
        int updated = jdbc.update("""
                UPDATE order_service.orders_current
                SET status = ?, aggregate_version = ?, updated_at = CURRENT_TIMESTAMP
                WHERE order_id = ? AND aggregate_version = ?
                """, status, event.aggregateVersion(), event.aggregateId(), event.aggregateVersion() - 1);
        ensureAppliedOrAlreadyProjected(event, updated);
    }

    private void applyAssetReservationConfirmed(ProjectionEvent event) {
        int updated = jdbc.update("""
                UPDATE order_service.orders_current
                SET asset_reservation_status = 'SUCCEEDED',
                    status = CASE WHEN status = 'PENDING_ASSET_CHECK' THEN 'OPEN' ELSE status END,
                    aggregate_version = ?, updated_at = CURRENT_TIMESTAMP
                WHERE order_id = ? AND aggregate_version = ?
                """, event.aggregateVersion(), event.aggregateId(), event.aggregateVersion() - 1);
        ensureAppliedOrAlreadyProjected(event, updated);
    }

    private void applyAssetReservationFailed(ProjectionEvent event) {
        int updated = jdbc.update("""
                UPDATE order_service.orders_current
                SET asset_reservation_status = 'REJECTED',
                    status = 'REJECTED',
                    aggregate_version = ?, updated_at = CURRENT_TIMESTAMP
                WHERE order_id = ? AND aggregate_version = ?
                """, event.aggregateVersion(), event.aggregateId(), event.aggregateVersion() - 1);
        ensureAppliedOrAlreadyProjected(event, updated);
    }

    private void applyCancellationCompleted(ProjectionEvent event) {
        int updated = jdbc.update("""
                UPDATE order_service.orders_current
                SET asset_reservation_status = 'RELEASED',
                    status = 'CANCELLED',
                    aggregate_version = ?, updated_at = CURRENT_TIMESTAMP
                WHERE order_id = ? AND aggregate_version = ?
                """, event.aggregateVersion(), event.aggregateId(), event.aggregateVersion() - 1);
        ensureAppliedOrAlreadyProjected(event, updated);
    }

    private void advanceVersion(ProjectionEvent event) {
        int updated = jdbc.update("""
                UPDATE order_service.orders_current
                SET aggregate_version = ?, updated_at = CURRENT_TIMESTAMP
                WHERE order_id = ? AND aggregate_version = ?
                """, event.aggregateVersion(), event.aggregateId(), event.aggregateVersion() - 1);
        ensureAppliedOrAlreadyProjected(event, updated);
    }

    private void applyMatched(ProjectionEvent event, OrderMatchedV1 matched) {
        int updated = jdbc.update("""
                UPDATE order_service.orders_current
                SET matched_amount = matched_amount + ?,
                    remaining_amount = remaining_amount - ?,
                    asset_reservation_status = 'SUCCEEDED',
                    status = CASE WHEN remaining_amount - ? = 0 THEN 'MATCHED'
                                  ELSE 'PARTIALLY_MATCHED' END,
                    aggregate_version = ?, updated_at = CURRENT_TIMESTAMP
                WHERE order_id = ? AND aggregate_version = ? AND remaining_amount >= ?
                """, matched.amount(), matched.amount(), matched.amount(), event.aggregateVersion(),
                event.aggregateId(), event.aggregateVersion() - 1, matched.amount());
        ensureAppliedOrAlreadyProjected(event, updated);
    }

    private void ensureAppliedOrAlreadyProjected(ProjectionEvent event, int affectedRows) {
        if (affectedRows == 1) {
            return;
        }
        Long version = currentProjectionVersion(event.aggregateId());
        if (version == null) {
            rebuildProjectionThrough(event);
            return;
        }
        if (version == null || version < event.aggregateVersion()) {
            throw new IllegalStateException("Projection version gap for order " + event.aggregateId());
        }
    }

    private Long currentProjectionVersion(UUID orderId) {
        try {
            return jdbc.queryForObject(
                    "SELECT aggregate_version FROM order_service.orders_current WHERE order_id = ?",
                    Long.class, orderId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    private void rebuildProjectionThrough(ProjectionEvent failedEvent) {
        List<ProjectionEvent> stream = jdbc.query("""
                SELECT global_position, aggregate_id, aggregate_version,
                       event_type, payload_canonical
                FROM order_service.order_event_store
                WHERE aggregate_id = ? AND aggregate_version <= ?
                ORDER BY aggregate_version
                """, (rs, rowNum) -> new ProjectionEvent(
                rs.getLong("global_position"),
                rs.getObject("aggregate_id", UUID.class),
                rs.getLong("aggregate_version"),
                rs.getString("event_type"),
                rs.getString("payload_canonical")),
                failedEvent.aggregateId(), failedEvent.aggregateVersion());
        ProjectionSnapshot snapshot = buildSnapshot(failedEvent, stream);
        upsertSnapshot(snapshot);
    }

    private ProjectionSnapshot buildSnapshot(ProjectionEvent failedEvent, List<ProjectionEvent> stream) {
        ProjectionSnapshot snapshot = null;
        for (ProjectionEvent event : stream) {
            try {
                switch (event.eventType()) {
                    case "OrderSubmissionRequestedV1" -> {
                        OrderSubmissionRequestedV1 requested =
                                objectMapper.readValue(event.payload(), OrderSubmissionRequestedV1.class);
                        snapshot = new ProjectionSnapshot(
                                requested.orderId(),
                                requested.userId(),
                                requested.marketId(),
                                requested.marketSequence(),
                                requested.side(),
                                requested.price(),
                                requested.amount(),
                                requested.amount(),
                                0,
                                "PENDING_ASSET_CHECK",
                                "PENDING",
                                event.aggregateVersion(),
                                requested.createdAt());
                    }
                    case "OrderAssetReservationConfirmedV1" -> {
                        snapshot = requireSnapshot(failedEvent, snapshot).withReservationConfirmed(event.aggregateVersion());
                    }
                    case "OrderAssetReservationFailedV1" -> {
                        snapshot = requireSnapshot(failedEvent, snapshot).withReservationRejected(event.aggregateVersion());
                    }
                    case "OrderMatchedV1" -> {
                        OrderMatchedV1 matched = objectMapper.readValue(event.payload(), OrderMatchedV1.class);
                        ProjectionSnapshot current = requireSnapshot(failedEvent, snapshot);
                        int matchedAmount = current.matchedAmount() + matched.amount();
                        int remainingAmount = current.remainingAmount() - matched.amount();
                        String status = remainingAmount == 0 ? "MATCHED" : "PARTIALLY_MATCHED";
                        snapshot = current.withAmounts(matchedAmount, remainingAmount, status, event.aggregateVersion());
                    }
                    case "OrderCancellationRequestedV1" -> {
                        ProjectionSnapshot current = requireSnapshot(failedEvent, snapshot);
                        snapshot = current.withStatus(current.status(), event.aggregateVersion());
                    }
                    case "OrderCancellationAcceptedV1" -> {
                        snapshot = requireSnapshot(failedEvent, snapshot)
                                .withStatus("CANCELLING", event.aggregateVersion());
                    }
                    case "OrderCancellationCompletedV1" -> {
                        snapshot = requireSnapshot(failedEvent, snapshot)
                                .withReservationReleased(event.aggregateVersion());
                    }
                    case "OrderCancelledV1" -> {
                        snapshot = requireSnapshot(failedEvent, snapshot).withReservationReleased(event.aggregateVersion());
                    }
                    default -> throw new IllegalArgumentException("Unsupported projection event: " + event.eventType());
                }
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Projection rebuild failed for order=" + failedEvent.aggregateId()
                                + ", failedGlobalPosition=" + failedEvent.globalPosition(), e);
            }
        }
        return requireSnapshot(failedEvent, snapshot);
    }

    private ProjectionSnapshot requireSnapshot(ProjectionEvent failedEvent, ProjectionSnapshot snapshot) {
        if (snapshot == null) {
            throw new IllegalStateException("Projection cannot rebuild missing order row before request event: "
                    + failedEvent.aggregateId());
        }
        return snapshot;
    }

    private void upsertSnapshot(ProjectionSnapshot snapshot) {
        jdbc.update("""
                INSERT INTO order_service.orders_current
                    (order_id, user_id, market_id, market_sequence, side, price,
                     original_amount, remaining_amount, matched_amount, status, asset_reservation_status,
                     aggregate_version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (order_id) DO UPDATE
                SET remaining_amount = EXCLUDED.remaining_amount,
                    matched_amount = EXCLUDED.matched_amount,
                    status = EXCLUDED.status,
                    asset_reservation_status = EXCLUDED.asset_reservation_status,
                    aggregate_version = EXCLUDED.aggregate_version,
                    updated_at = CURRENT_TIMESTAMP
                WHERE order_service.orders_current.aggregate_version < EXCLUDED.aggregate_version
                """, snapshot.orderId(), snapshot.userId(), snapshot.marketId(), snapshot.marketSequence(),
                snapshot.side(), snapshot.price(), snapshot.originalAmount(), snapshot.remainingAmount(),
                snapshot.matchedAmount(), snapshot.status(), snapshot.assetReservationStatus(),
                snapshot.aggregateVersion(), snapshot.createdAt());
    }

    private void repairStaleProjections() {
        List<ProjectionRepairTarget> targets = jdbc.query("""
                SELECT h.aggregate_id, h.current_version
                FROM order_service.order_stream_heads h
                LEFT JOIN order_service.orders_current oc ON oc.order_id = h.aggregate_id
                WHERE oc.order_id IS NULL OR oc.aggregate_version < h.current_version
                ORDER BY h.updated_at
                LIMIT ?
                """, (rs, rowNum) -> new ProjectionRepairTarget(
                rs.getObject("aggregate_id", UUID.class),
                rs.getLong("current_version")), repairBatchSize);
        for (ProjectionRepairTarget target : targets) {
            rebuildProjectionThrough(new ProjectionEvent(
                    0,
                    target.aggregateId(),
                    target.currentVersion(),
                    "ProjectionRepair",
                    "{}"));
        }
    }

    private record ProjectionEvent(
            long globalPosition,
            UUID aggregateId,
            long aggregateVersion,
            String eventType,
            String payload) {
    }

    private record ProjectionSnapshot(
            UUID orderId,
            UUID userId,
            String marketId,
            long marketSequence,
            String side,
            int price,
            int originalAmount,
            int remainingAmount,
            int matchedAmount,
            String status,
            String assetReservationStatus,
            long aggregateVersion,
            java.time.LocalDateTime createdAt) {

        ProjectionSnapshot withStatus(String status, long aggregateVersion) {
            return new ProjectionSnapshot(orderId, userId, marketId, marketSequence, side, price,
                    originalAmount, remainingAmount, matchedAmount, status, assetReservationStatus,
                    aggregateVersion, createdAt);
        }

        ProjectionSnapshot withAmounts(int matchedAmount, int remainingAmount, String status, long aggregateVersion) {
            return new ProjectionSnapshot(orderId, userId, marketId, marketSequence, side, price,
                    originalAmount, remainingAmount, matchedAmount, status, "SUCCEEDED",
                    aggregateVersion, createdAt);
        }

        ProjectionSnapshot withReservationConfirmed(long aggregateVersion) {
            String nextStatus = "PENDING_ASSET_CHECK".equals(status) ? "OPEN" : status;
            return new ProjectionSnapshot(orderId, userId, marketId, marketSequence, side, price,
                    originalAmount, remainingAmount, matchedAmount, nextStatus, "SUCCEEDED",
                    aggregateVersion, createdAt);
        }

        ProjectionSnapshot withReservationRejected(long aggregateVersion) {
            return new ProjectionSnapshot(orderId, userId, marketId, marketSequence, side, price,
                    originalAmount, remainingAmount, matchedAmount, "REJECTED", "REJECTED",
                    aggregateVersion, createdAt);
        }

        ProjectionSnapshot withReservationReleased(long aggregateVersion) {
            return new ProjectionSnapshot(orderId, userId, marketId, marketSequence, side, price,
                    originalAmount, remainingAmount, matchedAmount, "CANCELLED", "RELEASED",
                    aggregateVersion, createdAt);
        }
    }

    private record ProjectionRepairTarget(UUID aggregateId, long currentVersion) {
    }
}
