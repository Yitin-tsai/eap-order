package com.eap.eap_order.application;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class OrderSubmissionMetrics {

    private final Timer totalDuration;
    private final Timer backpressureDuration;
    private final Timer marketSequenceDuration;
    private final Timer buildEventDuration;
    private final Timer eventStoreRequestDuration;
    private final Timer rateLimitCheckDuration;

    public OrderSubmissionMetrics(MeterRegistry registry) {
        this.totalDuration = phaseTimer(registry, "total", "Total time spent accepting an order submission");
        this.backpressureDuration = phaseTimer(registry, "backpressure_check",
                "Time spent checking order-submission backpressure");
        this.marketSequenceDuration = phaseTimer(registry, "market_sequence",
                "Time spent allocating an order market sequence");
        this.buildEventDuration = phaseTimer(registry, "build_event",
                "Time spent building the OrderSubmitted integration event");
        this.eventStoreRequestDuration = phaseTimer(registry, "event_store_request",
                "Time spent appending the initial order request to event store and outbox");
        this.rateLimitCheckDuration = phaseTimer(registry, "rate_limit_check",
                "Time spent checking the order API rate limit");
    }

    public void recordTotal(Duration duration) {
        totalDuration.record(duration);
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
