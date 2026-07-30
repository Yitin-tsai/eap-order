package com.eap.eap_order.application;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class OrderSubmissionMetrics {

    private final Timer totalDuration;
    private final Timer buyControllerDuration;
    private final Timer sellControllerDuration;
    private final Timer controllerAfterServiceDuration;
    private final Timer preEventStoreDuration;
    private final Timer backpressureDuration;
    private final Timer marketSequenceDuration;
    private final Timer buildEventDuration;
    private final Timer eventStoreRequestDuration;
    private final Timer rateLimitAspectDuration;
    private final Timer rateLimitKeyExtractionDuration;
    private final Timer rateLimitCheckDuration;

    public OrderSubmissionMetrics(MeterRegistry registry) {
        this.totalDuration = phaseTimer(registry, "total", "Total time spent accepting an order submission");
        this.buyControllerDuration = phaseTimer(registry, "controller_buy_total",
                "Time spent inside the buy-order controller method");
        this.sellControllerDuration = phaseTimer(registry, "controller_sell_total",
                "Time spent inside the sell-order controller method");
        this.controllerAfterServiceDuration = phaseTimer(registry, "controller_after_service",
                "Time spent in the order controller after service completion");
        this.preEventStoreDuration = phaseTimer(registry, "pre_event_store",
                "Time spent before the initial order event-store append request");
        this.backpressureDuration = phaseTimer(registry, "backpressure_check",
                "Time spent checking order-submission backpressure");
        this.marketSequenceDuration = phaseTimer(registry, "market_sequence",
                "Time spent allocating an order market sequence");
        this.buildEventDuration = phaseTimer(registry, "build_event",
                "Time spent building the OrderSubmitted integration event");
        this.eventStoreRequestDuration = phaseTimer(registry, "event_store_request",
                "Time spent appending the initial order request to event store and outbox");
        this.rateLimitAspectDuration = phaseTimer(registry, "rate_limit_aspect",
                "Time spent in the order API rate-limit aspect");
        this.rateLimitKeyExtractionDuration = phaseTimer(registry, "rate_limit_key_extraction",
                "Time spent extracting the order API rate-limit key");
        this.rateLimitCheckDuration = phaseTimer(registry, "rate_limit_check",
                "Time spent checking the order API rate limit");
    }

    public void recordTotal(Duration duration) {
        totalDuration.record(duration);
    }

    public void recordController(String side, Duration duration) {
        if ("BUY".equalsIgnoreCase(side)) {
            buyControllerDuration.record(duration);
            return;
        }
        sellControllerDuration.record(duration);
    }

    public void recordControllerAfterService(Duration duration) {
        controllerAfterServiceDuration.record(duration);
    }

    public void recordPreEventStore(Duration duration) {
        preEventStoreDuration.record(duration);
    }

    public void recordBackpressure(Duration duration) {
        backpressureDuration.record(duration);
    }

    public void recordMarketSequence(Duration duration) {
        marketSequenceDuration.record(duration);
    }

    public void recordBuildEvent(Duration duration) {
        buildEventDuration.record(duration);
    }

    public void recordEventStoreRequest(Duration duration) {
        eventStoreRequestDuration.record(duration);
    }

    public void recordRateLimitAspect(Duration duration) {
        rateLimitAspectDuration.record(duration);
    }

    public void recordRateLimitKeyExtraction(Duration duration) {
        rateLimitKeyExtractionDuration.record(duration);
    }

    public void recordRateLimitCheck(Duration duration) {
        rateLimitCheckDuration.record(duration);
    }

    private Timer phaseTimer(MeterRegistry registry, String phase, String description) {
        return Timer.builder("eap_order_submission_phase_duration")
                .tag("phase", phase)
                .description(description)
                .publishPercentileHistogram()
                .register(registry);
    }
}
