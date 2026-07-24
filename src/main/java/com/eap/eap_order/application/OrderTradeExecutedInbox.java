package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

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
                     :dealPrice, :quantity, :payload, 'FAILED_RETRYABLE', 1,
                     CURRENT_TIMESTAMP, :lastError, CURRENT_TIMESTAMP)
                ON CONFLICT (trade_id) DO UPDATE
                SET attempt_count = CASE
                        WHEN inbox.status = 'APPLIED'
                            THEN inbox.attempt_count
                        ELSE inbox.attempt_count + 1
                    END,
                    status = CASE
                        WHEN inbox.status = 'APPLIED'
                            THEN inbox.status
                        ELSE 'FAILED_RETRYABLE'
                    END,
                    last_error = CASE
                        WHEN inbox.status = 'APPLIED'
                            THEN inbox.last_error
                        ELSE EXCLUDED.last_error
                    END,
                    payload = CASE
                        WHEN inbox.status = 'APPLIED'
                            THEN inbox.payload
                        ELSE EXCLUDED.payload
                    END,
                    updated_at = CURRENT_TIMESTAMP
                """, events.stream()
                .map(event -> new MapSqlParameterSource()
                        .addValues(params(event).getValues())
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

    private String truncate(String value) {
        if (value == null || value.length() <= ERROR_LIMIT) {
            return value;
        }
        return value.substring(0, ERROR_LIMIT);
    }
}
