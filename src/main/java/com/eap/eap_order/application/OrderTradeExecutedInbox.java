package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class OrderTradeExecutedInbox {

    private static final int ERROR_LIMIT = 2_000;

    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public OrderTradeExecutedInbox(
            @Qualifier("orderConsumerNamedParameterJdbcTemplate") NamedParameterJdbcTemplate jdbc,
            ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public void markFailed(List<TradeExecutedEvent> events, Exception failure) {
        markRetryable(events, failure);
    }

    public void markRetryable(List<TradeExecutedEvent> events, Exception failure) {
        upsert(events, "FAILED_RETRYABLE", failure);
    }

    public void markPending(List<TradeExecutedEvent> events, Exception failure) {
        upsert(events, "PENDING_PREREQUISITE", failure);
    }

    public void markPermanentFailure(List<TradeExecutedEvent> events, Exception failure) {
        upsert(events, "FAILED_PERMANENT", failure);
    }

    public List<InboxEntry> claimRetryable(int limit, String owner, long leaseMs) {
        return jdbc.query("""
                WITH candidates AS (
                    SELECT trade_id
                    FROM order_service.order_trade_execution_inbox
                    WHERE (status IN ('PENDING_PREREQUISITE', 'FAILED_RETRYABLE')
                               AND next_retry_at <= CURRENT_TIMESTAMP)
                       OR (status = 'IN_PROGRESS' AND claim_until <= CURRENT_TIMESTAMP)
                    ORDER BY next_retry_at, updated_at, trade_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT :limit
                )
                UPDATE order_service.order_trade_execution_inbox AS inbox
                SET status = 'IN_PROGRESS',
                    attempt_count = inbox.attempt_count + 1,
                    claimed_by = :owner,
                    claim_until = CURRENT_TIMESTAMP + (:leaseMs * INTERVAL '1 millisecond'),
                    updated_at = CURRENT_TIMESTAMP
                FROM candidates
                WHERE inbox.trade_id = candidates.trade_id
                RETURNING inbox.payload, inbox.attempt_count
                """, new MapSqlParameterSource()
                        .addValue("limit", limit)
                        .addValue("owner", owner)
                        .addValue("leaseMs", leaseMs),
                (rs, rowNum) -> new InboxEntry(
                        deserialize(rs.getString("payload")),
                        rs.getInt("attempt_count")));
    }

    public boolean markApplied(TradeExecutedEvent event, String owner) {
        return jdbc.update("""
                UPDATE order_service.order_trade_execution_inbox
                SET status = 'APPLIED', applied_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP, last_error = NULL,
                    claimed_by = NULL, claim_until = NULL
                WHERE trade_id = :tradeId AND status = 'IN_PROGRESS' AND claimed_by = :owner
                """, new MapSqlParameterSource()
                        .addValue("tradeId", event.getTradeId())
                        .addValue("owner", owner)) == 1;
    }

    public void reschedule(
            InboxEntry entry,
            String owner,
            String status,
            Exception failure,
            long delayMs) {
        jdbc.update("""
                UPDATE order_service.order_trade_execution_inbox
                SET status = :status,
                    next_retry_at = CURRENT_TIMESTAMP + (:delayMs * INTERVAL '1 millisecond'),
                    last_error = :lastError,
                    claimed_by = NULL,
                    claim_until = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE trade_id = :tradeId AND status = 'IN_PROGRESS' AND claimed_by = :owner
                """, new MapSqlParameterSource()
                        .addValue("tradeId", entry.event().getTradeId())
                        .addValue("owner", owner)
                        .addValue("status", status)
                        .addValue("delayMs", delayMs)
                        .addValue("lastError", truncate(failure.toString())));
    }

    public void markClaimedPermanentFailure(InboxEntry entry, String owner, Exception failure) {
        jdbc.update("""
                UPDATE order_service.order_trade_execution_inbox
                SET status = 'FAILED_PERMANENT',
                    last_error = :lastError,
                    claimed_by = NULL,
                    claim_until = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE trade_id = :tradeId AND status = 'IN_PROGRESS' AND claimed_by = :owner
                """, new MapSqlParameterSource()
                        .addValue("tradeId", entry.event().getTradeId())
                        .addValue("owner", owner)
                        .addValue("lastError", truncate(failure.toString())));
    }

    public Map<String, Long> countByStatus() {
        return jdbc.query("""
                SELECT status, COUNT(*) AS rows
                FROM order_service.order_trade_execution_inbox
                GROUP BY status
                """, rs -> {
                    Map<String, Long> counts = new java.util.HashMap<>();
                    while (rs.next()) {
                        counts.put(rs.getString("status"), rs.getLong("rows"));
                    }
                    return Map.copyOf(counts);
                });
    }

    public boolean retryPermanentFailure(String tradeId) {
        return jdbc.update("""
                UPDATE order_service.order_trade_execution_inbox
                SET status = 'FAILED_RETRYABLE',
                    next_retry_at = CURRENT_TIMESTAMP,
                    claimed_by = NULL,
                    claim_until = NULL,
                    last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE trade_id = :tradeId AND status = 'FAILED_PERMANENT'
                """, new MapSqlParameterSource("tradeId", tradeId)) == 1;
    }

    private void upsert(List<TradeExecutedEvent> events, String status, Exception failure) {
        if (events == null || events.isEmpty()) {
            return;
        }
        String error = truncate(failure == null ? "unknown failure" : failure.toString());
        jdbc.batchUpdate("""
                INSERT INTO order_service.order_trade_execution_inbox AS inbox
                    (trade_id, legacy_match_id, buyer_order_id, seller_order_id,
                     deal_price, quantity, payload, status, attempt_count,
                     received_at, last_error, updated_at)
                VALUES
                    (:tradeId, :legacyMatchId, :buyerOrderId, :sellerOrderId,
                     :dealPrice, :quantity, :payload, :status, 1,
                     CURRENT_TIMESTAMP, :lastError, CURRENT_TIMESTAMP)
                ON CONFLICT (trade_id) DO UPDATE
                SET attempt_count = CASE
                        WHEN inbox.status IN ('APPLIED', 'FAILED_PERMANENT')
                            THEN inbox.attempt_count
                        ELSE inbox.attempt_count + 1
                    END,
                    status = CASE
                        WHEN inbox.status IN ('APPLIED', 'FAILED_PERMANENT')
                            THEN inbox.status
                        ELSE EXCLUDED.status
                    END,
                    last_error = CASE
                        WHEN inbox.status IN ('APPLIED', 'FAILED_PERMANENT')
                            THEN inbox.last_error
                        ELSE EXCLUDED.last_error
                    END,
                    payload = CASE
                        WHEN inbox.status IN ('APPLIED', 'FAILED_PERMANENT')
                            THEN inbox.payload
                        ELSE EXCLUDED.payload
                    END,
                    next_retry_at = CASE
                        WHEN inbox.status IN ('APPLIED', 'FAILED_PERMANENT')
                            THEN inbox.next_retry_at
                        ELSE CURRENT_TIMESTAMP
                    END,
                    claimed_by = NULL,
                    claim_until = NULL,
                    updated_at = CURRENT_TIMESTAMP
                """, events.stream()
                        .map(event -> new MapSqlParameterSource()
                                .addValues(params(event).getValues())
                                .addValue("status", status)
                                .addValue("lastError", error))
                .toArray(MapSqlParameterSource[]::new));
    }

    private MapSqlParameterSource params(TradeExecutedEvent event) {
        return new MapSqlParameterSource()
                .addValue("tradeId", event.getTradeId())
                .addValue("legacyMatchId", event.getLegacyMatchId())
                .addValue("buyerOrderId", event.getBuyerOrderId())
                .addValue("sellerOrderId", event.getSellerOrderId())
                .addValue("dealPrice", event.getDealPrice())
                .addValue("quantity", event.getQuantity())
                .addValue("payload", payload(event));
    }

    private String payload(TradeExecutedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize TradeExecutedEvent for Order inbox: tradeId="
                    + event.getTradeId(), e);
        }
    }

    private TradeExecutedEvent deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, TradeExecutedEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize pending TradeExecuted inbox payload", e);
        }
    }

    private String truncate(String value) {
        if (value == null || value.length() <= ERROR_LIMIT) {
            return value;
        }
        return value.substring(0, ERROR_LIMIT);
    }

    public record InboxEntry(TradeExecutedEvent event, int attemptCount) {
    }
}
