package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationSucceededEvent;
import com.eap.common.event.OrderFailedEvent;
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
import java.util.Map;
import java.util.UUID;

@Component
public class OrderAssetReservationResultInbox {

    private static final int ERROR_LIMIT = 2_000;

    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public OrderAssetReservationResultInbox(
            @Qualifier("orderConsumerNamedParameterJdbcTemplate") NamedParameterJdbcTemplate jdbc,
            ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public ReceiveOutcome receiveConfirmed(OrderAssetReservationSucceededEvent event) {
        if (event == null || event.getOrderId() == null || event.getUserId() == null) {
            throw new IllegalArgumentException("OrderAssetReservationSucceededEvent orderId and userId are required");
        }
        return receive(event.getOrderId(), ResultType.CONFIRMED, serialize(event));
    }

    public ReceiveOutcome receiveFailed(OrderFailedEvent event) {
        if (event == null || event.getOrderId() == null || event.getUserId() == null) {
            throw new IllegalArgumentException("OrderFailedEvent orderId and userId are required");
        }
        return receive(event.getOrderId(), ResultType.FAILED, serialize(event));
    }

    private ReceiveOutcome receive(UUID orderId, ResultType resultType, String payload) {
        String payloadHash = sha256(payload);
        int inserted = jdbc.update("""
                INSERT INTO order_service.order_asset_reservation_result_inbox
                    (order_id, result_type, payload, payload_hash, schema_version,
                     status, attempt_count, next_retry_at, received_at, updated_at)
                VALUES
                    (:orderId, :resultType, :payload, :payloadHash, 1,
                     'PENDING', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (order_id) DO NOTHING
                """, new MapSqlParameterSource()
                .addValue("orderId", orderId)
                .addValue("resultType", resultType.name())
                .addValue("payload", payload)
                .addValue("payloadHash", payloadHash));
        if (inserted == 1) {
            return ReceiveOutcome.ACCEPTED;
        }

        ExistingResult existing = jdbc.queryForObject("""
                SELECT result_type, payload_hash
                FROM order_service.order_asset_reservation_result_inbox
                WHERE order_id = :orderId
                """, new MapSqlParameterSource("orderId", orderId),
                (rs, rowNum) -> new ExistingResult(
                        ResultType.valueOf(rs.getString("result_type")),
                        rs.getString("payload_hash")));
        if (existing != null
                && existing.resultType() == resultType
                && existing.payloadHash().equals(payloadHash)) {
            return ReceiveOutcome.DUPLICATE;
        }

        String error = "Reservation result identity conflict: orderId=" + orderId
                + ", existingType=" + (existing == null ? "UNKNOWN" : existing.resultType())
                + ", incomingType=" + resultType;
        jdbc.update("""
                UPDATE order_service.order_asset_reservation_result_inbox
                SET status = CASE WHEN status = 'APPLIED' THEN status ELSE 'FAILED_PERMANENT' END,
                    error_type = 'IDENTITY_CONFLICT',
                    last_error = :lastError,
                    conflict_detected_at = CURRENT_TIMESTAMP,
                    conflicting_result_type = :resultType,
                    conflicting_payload = :payload,
                    claimed_by = CASE WHEN status = 'APPLIED' THEN claimed_by ELSE NULL END,
                    claim_until = CASE WHEN status = 'APPLIED' THEN claim_until ELSE NULL END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE order_id = :orderId
                """, new MapSqlParameterSource()
                .addValue("orderId", orderId)
                .addValue("resultType", resultType.name())
                .addValue("payload", payload)
                .addValue("lastError", error));
        return ReceiveOutcome.CONFLICT;
    }

    public List<InboxEntry> claimRetryable(int limit, String owner, long leaseMs) {
        return jdbc.query("""
                WITH candidates AS (
                    SELECT order_id
                    FROM order_service.order_asset_reservation_result_inbox
                    WHERE (status IN ('PENDING', 'FAILED_RETRYABLE')
                               AND next_retry_at <= CURRENT_TIMESTAMP)
                       OR (status = 'IN_PROGRESS' AND claim_until <= CURRENT_TIMESTAMP)
                    ORDER BY next_retry_at, updated_at, order_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT :limit
                )
                UPDATE order_service.order_asset_reservation_result_inbox AS inbox
                SET status = 'IN_PROGRESS',
                    attempt_count = inbox.attempt_count + 1,
                    claimed_by = :owner,
                    claim_until = CURRENT_TIMESTAMP + (:leaseMs * INTERVAL '1 millisecond'),
                    updated_at = CURRENT_TIMESTAMP
                FROM candidates
                WHERE inbox.order_id = candidates.order_id
                RETURNING inbox.order_id, inbox.result_type, inbox.payload, inbox.attempt_count
                """, new MapSqlParameterSource()
                .addValue("limit", limit)
                .addValue("owner", owner)
                .addValue("leaseMs", leaseMs),
                (rs, rowNum) -> entry(
                        rs.getObject("order_id", UUID.class),
                        ResultType.valueOf(rs.getString("result_type")),
                        rs.getString("payload"),
                        rs.getInt("attempt_count")));
    }

    public boolean markApplied(InboxEntry entry, String owner) {
        return jdbc.update("""
                UPDATE order_service.order_asset_reservation_result_inbox
                SET status = 'APPLIED', applied_at = CURRENT_TIMESTAMP,
                    claimed_by = NULL, claim_until = NULL,
                    error_type = NULL, last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE order_id = :orderId
                  AND status = 'IN_PROGRESS' AND claimed_by = :owner
                """, claimParams(entry, owner)) == 1;
    }

    public boolean reschedule(
            InboxEntry entry,
            String owner,
            String errorType,
            Exception failure,
            long delayMs) {
        return jdbc.update("""
                UPDATE order_service.order_asset_reservation_result_inbox
                SET status = 'FAILED_RETRYABLE',
                    next_retry_at = CURRENT_TIMESTAMP + (:delayMs * INTERVAL '1 millisecond'),
                    claimed_by = NULL, claim_until = NULL,
                    error_type = :errorType, last_error = :lastError,
                    updated_at = CURRENT_TIMESTAMP
                WHERE order_id = :orderId
                  AND status = 'IN_PROGRESS' AND claimed_by = :owner
                """, claimParams(entry, owner)
                .addValue("delayMs", delayMs)
                .addValue("errorType", errorType)
                .addValue("lastError", truncate(failure.toString()))) == 1;
    }

    public boolean markPermanent(
            InboxEntry entry,
            String owner,
            String errorType,
            Exception failure) {
        return jdbc.update("""
                UPDATE order_service.order_asset_reservation_result_inbox
                SET status = 'FAILED_PERMANENT',
                    claimed_by = NULL, claim_until = NULL,
                    error_type = :errorType, last_error = :lastError,
                    updated_at = CURRENT_TIMESTAMP
                WHERE order_id = :orderId
                  AND status = 'IN_PROGRESS' AND claimed_by = :owner
                """, claimParams(entry, owner)
                .addValue("errorType", errorType)
                .addValue("lastError", truncate(failure.toString()))) == 1;
    }

    public Map<String, Long> countByStatus() {
        return jdbc.query("""
                SELECT status, COUNT(*) AS rows
                FROM order_service.order_asset_reservation_result_inbox
                GROUP BY status
                """, rs -> {
                    Map<String, Long> counts = new java.util.HashMap<>();
                    while (rs.next()) {
                        counts.put(rs.getString("status"), rs.getLong("rows"));
                    }
                    return Map.copyOf(counts);
                });
    }

    public long countConflicts() {
        Long result = jdbc.queryForObject("""
                SELECT COUNT(*)
                FROM order_service.order_asset_reservation_result_inbox
                WHERE conflict_detected_at IS NOT NULL
                """, Map.of(), Long.class);
        return result == null ? 0 : result;
    }

    public boolean retryPermanentFailure(UUID orderId) {
        return jdbc.update("""
                UPDATE order_service.order_asset_reservation_result_inbox
                SET status = 'FAILED_RETRYABLE',
                    next_retry_at = CURRENT_TIMESTAMP,
                    claimed_by = NULL, claim_until = NULL,
                    error_type = NULL, last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE order_id = :orderId
                  AND status = 'FAILED_PERMANENT'
                  AND conflict_detected_at IS NULL
                """, new MapSqlParameterSource("orderId", orderId)) == 1;
    }

    private InboxEntry entry(UUID orderId, ResultType resultType, String payload, int attemptCount) {
        return switch (resultType) {
            case CONFIRMED -> new InboxEntry(
                    orderId, resultType, deserialize(payload, OrderAssetReservationSucceededEvent.class), null, attemptCount);
            case FAILED -> new InboxEntry(
                    orderId, resultType, null, deserialize(payload, OrderFailedEvent.class), attemptCount);
        };
    }

    private MapSqlParameterSource claimParams(InboxEntry entry, String owner) {
        return new MapSqlParameterSource()
                .addValue("orderId", entry.orderId())
                .addValue("owner", owner);
    }

    private String serialize(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize asset reservation result", e);
        }
    }

    private <T> T deserialize(String payload, Class<T> type) {
        try {
            return objectMapper.readValue(payload, type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialize asset reservation result inbox payload", e);
        }
    }

    private String sha256(String payload) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(payload.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private String truncate(String value) {
        if (value == null || value.length() <= ERROR_LIMIT) {
            return value;
        }
        return value.substring(0, ERROR_LIMIT);
    }

    public enum ResultType {
        CONFIRMED,
        FAILED
    }

    public enum ReceiveOutcome {
        ACCEPTED,
        DUPLICATE,
        CONFLICT
    }

    public record InboxEntry(
            UUID orderId,
            ResultType resultType,
            OrderAssetReservationSucceededEvent confirmedEvent,
            OrderFailedEvent failedEvent,
            int attemptCount) {
    }

    private record ExistingResult(ResultType resultType, String payloadHash) {
    }
}
