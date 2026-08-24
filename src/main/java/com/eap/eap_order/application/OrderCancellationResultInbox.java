package com.eap.eap_order.application;

import com.eap.common.event.OrderCancellationResultEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Component
public class OrderCancellationResultInbox {

    private static final int ERROR_LIMIT = 2_000;

    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public OrderCancellationResultInbox(
            @Qualifier("orderConsumerNamedParameterJdbcTemplate") NamedParameterJdbcTemplate jdbc,
            ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public void receive(OrderCancellationResultEvent event) {
        String payload = serialize(event);
        jdbc.update("""
                INSERT INTO order_service.order_cancellation_result_inbox
                    (cancellation_id, order_id, payload, status, attempt_count,
                     next_retry_at, received_at, updated_at)
                VALUES
                    (:cancellationId, :orderId, :payload, 'PENDING', 0,
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (cancellation_id) DO NOTHING
                """, new MapSqlParameterSource()
                .addValue("cancellationId", event.getCancellationId())
                .addValue("orderId", event.getOrderId())
                .addValue("payload", payload));
        String existingPayload = jdbc.queryForObject("""
                SELECT payload
                FROM order_service.order_cancellation_result_inbox
                WHERE cancellation_id = :cancellationId
                """, new MapSqlParameterSource("cancellationId", event.getCancellationId()), String.class);
        if (!sameEvent(event, deserialize(existingPayload))) {
            throw new IllegalStateException("Cancellation result identity conflict: " + event.getCancellationId());
        }
    }

    public List<InboxEntry> claimRetryable(int limit, String owner, long leaseMs) {
        return jdbc.query("""
                WITH candidates AS (
                    SELECT cancellation_id
                    FROM order_service.order_cancellation_result_inbox
                    WHERE (status IN ('PENDING', 'PENDING_PREREQUISITE', 'FAILED_RETRYABLE')
                               AND next_retry_at <= CURRENT_TIMESTAMP)
                       OR (status = 'IN_PROGRESS' AND claim_until <= CURRENT_TIMESTAMP)
                    ORDER BY next_retry_at, updated_at, cancellation_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT :limit
                )
                UPDATE order_service.order_cancellation_result_inbox AS inbox
                SET status = 'IN_PROGRESS',
                    attempt_count = inbox.attempt_count + 1,
                    claimed_by = :owner,
                    claim_until = CURRENT_TIMESTAMP + (:leaseMs * INTERVAL '1 millisecond'),
                    updated_at = CURRENT_TIMESTAMP
                FROM candidates
                WHERE inbox.cancellation_id = candidates.cancellation_id
                RETURNING inbox.payload, inbox.attempt_count
                """, new MapSqlParameterSource()
                .addValue("limit", limit)
                .addValue("owner", owner)
                .addValue("leaseMs", leaseMs),
                (rs, rowNum) -> new InboxEntry(
                        deserialize(rs.getString("payload")),
                        rs.getInt("attempt_count")));
    }

    public boolean markApplied(InboxEntry entry, String owner) {
        return jdbc.update("""
                UPDATE order_service.order_cancellation_result_inbox
                SET status = 'APPLIED', applied_at = CURRENT_TIMESTAMP,
                    claimed_by = NULL, claim_until = NULL, last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE cancellation_id = :cancellationId
                  AND status = 'IN_PROGRESS' AND claimed_by = :owner
                """, claimParams(entry, owner)) == 1;
    }

    public void reschedule(
            InboxEntry entry,
            String owner,
            String status,
            Exception failure,
            long delayMs) {
        jdbc.update("""
                UPDATE order_service.order_cancellation_result_inbox
                SET status = :status,
                    next_retry_at = CURRENT_TIMESTAMP + (:delayMs * INTERVAL '1 millisecond'),
                    claimed_by = NULL, claim_until = NULL,
                    last_error = :lastError, updated_at = CURRENT_TIMESTAMP
                WHERE cancellation_id = :cancellationId
                  AND status = 'IN_PROGRESS' AND claimed_by = :owner
                """, claimParams(entry, owner)
                .addValue("status", status)
                .addValue("delayMs", delayMs)
                .addValue("lastError", truncate(failure.toString())));
    }

    public void markPermanent(InboxEntry entry, String owner, Exception failure) {
        jdbc.update("""
                UPDATE order_service.order_cancellation_result_inbox
                SET status = 'FAILED_PERMANENT',
                    claimed_by = NULL, claim_until = NULL,
                    last_error = :lastError, updated_at = CURRENT_TIMESTAMP
                WHERE cancellation_id = :cancellationId
                  AND status = 'IN_PROGRESS' AND claimed_by = :owner
                """, claimParams(entry, owner)
                .addValue("lastError", truncate(failure.toString())));
    }

    private MapSqlParameterSource claimParams(InboxEntry entry, String owner) {
        return new MapSqlParameterSource()
                .addValue("cancellationId", entry.event().getCancellationId())
                .addValue("owner", owner);
    }

    private String serialize(OrderCancellationResultEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize cancellation result", e);
        }
    }

    private OrderCancellationResultEvent deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, OrderCancellationResultEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialize cancellation result inbox payload", e);
        }
    }

    private boolean sameEvent(
            OrderCancellationResultEvent left,
            OrderCancellationResultEvent right) {
        return Objects.equals(left.getCancellationId(), right.getCancellationId())
                && Objects.equals(left.getOrderId(), right.getOrderId())
                && Objects.equals(left.getUserId(), right.getUserId())
                && Objects.equals(left.getOutcome(), right.getOutcome())
                && Objects.equals(left.getReason(), right.getReason())
                && Objects.equals(left.getOrderType(), right.getOrderType())
                && Objects.equals(left.getLimitPrice(), right.getLimitPrice())
                && Objects.equals(left.getCancelledAmount(), right.getCancelledAmount())
                && Objects.equals(left.getDecidedAt(), right.getDecidedAt());
    }

    private String truncate(String value) {
        return value.length() <= ERROR_LIMIT ? value : value.substring(0, ERROR_LIMIT);
    }

    public record InboxEntry(OrderCancellationResultEvent event, int attemptCount) {
    }
}
