package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationReleasedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

@Component
public class OrderAssetReservationReleasedInbox {

    private static final int ERROR_LIMIT = 2_000;

    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public OrderAssetReservationReleasedInbox(
            @Qualifier("orderConsumerNamedParameterJdbcTemplate") NamedParameterJdbcTemplate jdbc,
            ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public ReceiveOutcome receive(OrderAssetReservationReleasedEvent event) {
        validate(event);
        String payload = serialize(event);
        String hash = sha256(payload);
        int inserted = jdbc.update("""
                INSERT INTO order_service.order_asset_reservation_released_inbox
                    (cancellation_id, event_id, order_id, payload, payload_hash,
                     schema_version, status, attempt_count, next_retry_at, received_at, updated_at)
                VALUES
                    (:cancellationId, :eventId, :orderId, :payload, :payloadHash,
                     1, 'PENDING', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (cancellation_id) DO NOTHING
                """, params(event.getCancellationId())
                .addValue("eventId", event.getEventId())
                .addValue("orderId", event.getOrderId())
                .addValue("payload", payload)
                .addValue("payloadHash", hash));
        if (inserted == 1) {
            return ReceiveOutcome.ACCEPTED;
        }

        Existing existing = jdbc.queryForObject("""
                SELECT event_id, order_id, payload_hash
                FROM order_service.order_asset_reservation_released_inbox
                WHERE cancellation_id = :cancellationId
                """, params(event.getCancellationId()),
                (rs, rowNum) -> new Existing(
                        rs.getObject("event_id", UUID.class),
                        rs.getObject("order_id", UUID.class),
                        rs.getString("payload_hash")));
        if (existing != null
                && event.getEventId().equals(existing.eventId())
                && event.getOrderId().equals(existing.orderId())
                && hash.equals(existing.payloadHash())) {
            return ReceiveOutcome.DUPLICATE;
        }

        jdbc.update("""
                UPDATE order_service.order_asset_reservation_released_inbox
                SET status = CASE WHEN status = 'APPLIED' THEN status ELSE 'FAILED_PERMANENT' END,
                    error_type = 'IDENTITY_CONFLICT',
                    last_error = :lastError,
                    conflict_detected_at = CURRENT_TIMESTAMP,
                    conflicting_payload = :payload,
                    claimed_by = CASE WHEN status = 'APPLIED' THEN claimed_by ELSE NULL END,
                    claim_until = CASE WHEN status = 'APPLIED' THEN claim_until ELSE NULL END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE cancellation_id = :cancellationId
                """, params(event.getCancellationId())
                .addValue("payload", payload)
                .addValue("lastError", "Order asset-release identity conflict: cancellationId="
                        + event.getCancellationId()));
        return ReceiveOutcome.CONFLICT;
    }

    public List<InboxEntry> claimRetryable(int limit, String owner, long leaseMs) {
        return jdbc.query("""
                WITH candidates AS (
                    SELECT cancellation_id
                    FROM order_service.order_asset_reservation_released_inbox
                    WHERE (status IN ('PENDING', 'PENDING_PREREQUISITE', 'FAILED_RETRYABLE')
                               AND next_retry_at <= CURRENT_TIMESTAMP)
                       OR (status = 'IN_PROGRESS' AND claim_until <= CURRENT_TIMESTAMP)
                    ORDER BY next_retry_at, updated_at, cancellation_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT :limit
                )
                UPDATE order_service.order_asset_reservation_released_inbox AS inbox
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
                UPDATE order_service.order_asset_reservation_released_inbox
                SET status = 'APPLIED', applied_at = CURRENT_TIMESTAMP,
                    claimed_by = NULL, claim_until = NULL,
                    error_type = NULL, last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE cancellation_id = :cancellationId
                  AND status = 'IN_PROGRESS' AND claimed_by = :owner
                """, claimParams(entry, owner)) == 1;
    }

    public boolean reschedule(
            InboxEntry entry,
            String owner,
            String status,
            String errorType,
            Exception failure,
            long delayMs) {
        return jdbc.update("""
                UPDATE order_service.order_asset_reservation_released_inbox
                SET status = :status,
                    next_retry_at = CURRENT_TIMESTAMP + (:delayMs * INTERVAL '1 millisecond'),
                    claimed_by = NULL, claim_until = NULL,
                    error_type = :errorType, last_error = :lastError,
                    updated_at = CURRENT_TIMESTAMP
                WHERE cancellation_id = :cancellationId
                  AND status = 'IN_PROGRESS' AND claimed_by = :owner
                """, claimParams(entry, owner)
                .addValue("status", status)
                .addValue("errorType", errorType)
                .addValue("lastError", truncate(failure.toString()))
                .addValue("delayMs", delayMs)) == 1;
    }

    public boolean markPermanent(
            InboxEntry entry,
            String owner,
            String errorType,
            Exception failure) {
        return jdbc.update("""
                UPDATE order_service.order_asset_reservation_released_inbox
                SET status = 'FAILED_PERMANENT',
                    claimed_by = NULL, claim_until = NULL,
                    error_type = :errorType, last_error = :lastError,
                    updated_at = CURRENT_TIMESTAMP
                WHERE cancellation_id = :cancellationId
                  AND status = 'IN_PROGRESS' AND claimed_by = :owner
                """, claimParams(entry, owner)
                .addValue("errorType", errorType)
                .addValue("lastError", truncate(failure.toString()))) == 1;
    }

    private void validate(OrderAssetReservationReleasedEvent event) {
        if (event == null || event.getEventId() == null || event.getCancellationId() == null
                || event.getOrderId() == null || event.getUserId() == null
                || event.getReleasedQuantity() == null || event.getReleasedQuantity() <= 0) {
            throw new IllegalArgumentException("Released reservation event requires complete positive identity");
        }
    }

    private MapSqlParameterSource params(UUID cancellationId) {
        return new MapSqlParameterSource("cancellationId", cancellationId);
    }

    private MapSqlParameterSource claimParams(InboxEntry entry, String owner) {
        return params(entry.event().getCancellationId()).addValue("owner", owner);
    }

    private String serialize(OrderAssetReservationReleasedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize released reservation event", e);
        }
    }

    private OrderAssetReservationReleasedEvent deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, OrderAssetReservationReleasedEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot deserialize released reservation inbox payload", e);
        }
    }

    private String sha256(String payload) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private String truncate(String value) {
        return value == null || value.length() <= ERROR_LIMIT ? value : value.substring(0, ERROR_LIMIT);
    }

    public enum ReceiveOutcome {
        ACCEPTED,
        DUPLICATE,
        CONFLICT
    }

    public record InboxEntry(OrderAssetReservationReleasedEvent event, int attemptCount) {
    }

    private record Existing(UUID eventId, UUID orderId, String payloadHash) {
    }
}
