package com.eap.eap_order.application;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class OrderTradeBatchMetrics {

    private final MeterRegistry registry;
    private final Counter batches;
    private final Counter events;
    private final Counter batchAppliedEvents;
    private final Counter fallbackEvents;
    private final Counter overlapFallbackEvents;
    private final Map<String, Counter> batchSizes = new ConcurrentHashMap<>();
    private final Map<String, Counter> fallbackReasons = new ConcurrentHashMap<>();

    public OrderTradeBatchMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.batches = Counter.builder("eap_order_trade_batch_total")
                .description("Order TradeExecuted listener batches received")
                .register(registry);
        this.events = Counter.builder("eap_order_trade_batch_events_total")
                .description("Order TradeExecuted events received through the batch listener")
                .register(registry);
        this.batchAppliedEvents = Counter.builder("eap_order_trade_batch_applied_events_total")
                .description("Order TradeExecuted events applied through the non-overlap DB batch path")
                .register(registry);
        this.fallbackEvents = Counter.builder("eap_order_trade_batch_fallback_events_total")
                .description("Order TradeExecuted events handled by the single-event fallback path")
                .register(registry);
        this.overlapFallbackEvents = Counter.builder("eap_order_trade_batch_overlap_fallback_events_total")
                .description("Order TradeExecuted events that could not use DB batch because an order repeated in the batch")
                .register(registry);
    }

    void received(int eventCount) {
        batches.increment();
        events.increment(eventCount);
        batchSize(Integer.toString(eventCount)).increment();
    }

    void batchApplied(int eventCount) {
        batchAppliedEvents.increment(eventCount);
    }

    void fallback(int eventCount) {
        fallback("unknown", eventCount);
    }

    void fallback(String reason, int eventCount) {
        fallbackEvents.increment(eventCount);
        fallbackReason(reason).increment(eventCount);
    }

    void overlapFallback(int eventCount) {
        overlapFallbackEvents.increment(eventCount);
    }

    private Counter batchSize(String size) {
        return batchSizes.computeIfAbsent(size, key -> Counter.builder("eap_order_trade_batch_size_total")
                .description("Order TradeExecuted listener batches by exact event count")
                .tag("size", key)
                .register(registry));
    }

    private Counter fallbackReason(String reason) {
        return fallbackReasons.computeIfAbsent(reason, key -> Counter.builder("eap_order_trade_batch_fallback_reason_total")
                .description("Order TradeExecuted events handled by the single-event fallback path by reason")
                .tag("reason", key)
                .register(registry));
    }
}
