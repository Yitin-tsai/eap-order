package com.eap.eap_order.eventstore;

import com.eap.eap_order.domain.ordersourcing.OrderSubmissionRequestedV1;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;

@Component
public class OrderSubmissionHeadProjector {

    private static final String PROJECTION_NAME = "order_submission_stream_heads";

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final boolean enabled;
    private final int batchSize;
    private final int maxBatchesPerTick;

    public OrderSubmissionHeadProjector(
            @Qualifier("orderConsumerJdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            @Qualifier("orderConsumerTransactionManager") PlatformTransactionManager transactionManager,
            @Value("${eap.order.submission-head-projector.enabled:false}") boolean enabled,
            @Value("${eap.order.submission-head-projector.batch-size:500}") int batchSize,
            @Value("${eap.order.submission-head-projector.max-batches-per-tick:4}") int maxBatchesPerTick) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.enabled = enabled;
        this.batchSize = Math.max(1, batchSize);
        this.maxBatchesPerTick = Math.max(1, maxBatchesPerTick);
    }

    @Scheduled(fixedDelayString = "${eap.order.submission-head-projector.poll-interval-ms:25}")
    public void project() {
        if (!enabled) {
            return;
        }
        boolean fullBatch;
        int batches = 0;
        do {
            fullBatch = Boolean.TRUE.equals(transactionTemplate.execute(status -> projectBatch()));
            batches++;
        } while (fullBatch && batches < maxBatchesPerTick);
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
        List<SubmissionEvent> events = jdbc.query("""
                SELECT global_position, event_id, aggregate_id, event_type, hash, payload_canonical
                FROM order_service.order_event_store
                WHERE global_position > ?
                ORDER BY global_position
                LIMIT ?
                """, (rs, rowNum) -> new SubmissionEvent(
                rs.getLong("global_position"),
                rs.getObject("event_id", UUID.class),
                rs.getObject("aggregate_id", UUID.class),
                rs.getString("event_type"),
                rs.getString("hash"),
                rs.getString("payload_canonical")), checkpoint == null ? 0 : checkpoint, batchSize);
        events = contiguousPrefix(events, checkpoint == null ? 0 : checkpoint);

        long lastPosition = checkpoint == null ? 0 : checkpoint;
        for (SubmissionEvent event : events) {
            if ("OrderSubmissionRequestedV1".equals(event.eventType())) {
                apply(event);
            }
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

    private List<SubmissionEvent> contiguousPrefix(List<SubmissionEvent> events, long checkpoint) {
        List<SubmissionEvent> prefix = new java.util.ArrayList<>(events.size());
        long expectedPosition = checkpoint + 1;
        for (SubmissionEvent event : events) {
            if (event.globalPosition() != expectedPosition) {
                break;
            }
            prefix.add(event);
            expectedPosition++;
        }
        return prefix;
    }

    private void apply(SubmissionEvent event) {
        try {
            OrderSubmissionRequestedV1 requested =
                    objectMapper.readValue(event.payloadCanonical(), OrderSubmissionRequestedV1.class);
            int updated = jdbc.update("""
                    INSERT INTO order_service.order_stream_heads
                        (aggregate_id, current_version, last_event_id, last_hash,
                         user_id, remaining_amount, status, updated_at)
                    VALUES (?, 1, ?, ?, ?, ?, 'PENDING_ASSET_CHECK', CURRENT_TIMESTAMP)
                    ON CONFLICT (aggregate_id) DO UPDATE
                    SET current_version = GREATEST(order_service.order_stream_heads.current_version, 1),
                        last_event_id = CASE
                            WHEN order_service.order_stream_heads.current_version <= 1 THEN EXCLUDED.last_event_id
                            ELSE order_service.order_stream_heads.last_event_id
                        END,
                        last_hash = CASE
                            WHEN order_service.order_stream_heads.current_version <= 1 THEN EXCLUDED.last_hash
                            ELSE order_service.order_stream_heads.last_hash
                        END,
                        user_id = COALESCE(order_service.order_stream_heads.user_id, EXCLUDED.user_id),
                        remaining_amount = COALESCE(order_service.order_stream_heads.remaining_amount, EXCLUDED.remaining_amount),
                        status = COALESCE(order_service.order_stream_heads.status, EXCLUDED.status),
                        updated_at = CURRENT_TIMESTAMP
                    """, event.aggregateId(), event.eventId(), event.hash(),
                    requested.userId(), requested.amount());
            if (updated != 1) {
                throw new IllegalStateException("Order submission head projector affected rows=" + updated);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Order submission head projection failed at globalPosition="
                    + event.globalPosition(), e);
        }
    }

    private record SubmissionEvent(
            long globalPosition,
            UUID eventId,
            UUID aggregateId,
            String eventType,
            String hash,
            String payloadCanonical) {
    }
}
