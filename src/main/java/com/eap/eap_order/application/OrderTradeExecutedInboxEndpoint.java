package com.eap.eap_order.application;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Endpoint(id = "orderTradeInbox")
@ConditionalOnProperty(name = "eap.order.trade-execution-inbox-admin.enabled", havingValue = "true")
@RequiredArgsConstructor
public class OrderTradeExecutedInboxEndpoint {

    private final OrderTradeExecutedInbox inbox;

    @ReadOperation
    public Map<String, Long> status() {
        return inbox.countByStatus();
    }

    @WriteOperation
    public Map<String, Object> retry(String tradeId) {
        boolean accepted = inbox.retryPermanentFailure(tradeId);
        return Map.of("tradeId", tradeId, "accepted", accepted);
    }
}
