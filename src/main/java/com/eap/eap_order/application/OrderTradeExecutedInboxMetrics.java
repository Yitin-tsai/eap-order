package com.eap.eap_order.application;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class OrderTradeExecutedInboxMetrics {

    private static final List<String> STATUSES = List.of(
            "PENDING_PREREQUISITE",
            "FAILED_RETRYABLE",
            "IN_PROGRESS",
            "FAILED_PERMANENT");

    private final OrderTradeExecutedInbox inbox;
    private final MultiGauge statusRows;

    public OrderTradeExecutedInboxMetrics(OrderTradeExecutedInbox inbox, MeterRegistry registry) {
        this.inbox = inbox;
        this.statusRows = MultiGauge.builder("eap_order_trade_inbox_rows")
                .description("Current durable TradeExecuted inbox rows by actionable status")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${eap.order.trade-execution-inbox-metrics.poll-interval-ms:10000}")
    public void refresh() {
        Map<String, Long> counts = inbox.countByStatus();
        statusRows.register(
                STATUSES.stream()
                        .map(status -> MultiGauge.Row.of(
                                Tags.of("status", status), counts.getOrDefault(status, 0L)))
                        .toList(),
                true);
    }
}
