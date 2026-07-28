package com.eap.eap_order.eventstore;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
public class OrderSubmissionAppendMetrics {

    private final MeterRegistry registry;
    private final Map<String, Timer> timers = new ConcurrentHashMap<>();

    public OrderSubmissionAppendMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void record(String phase, long startedNanos) {
        timers.computeIfAbsent(phase, this::timer)
                .record(System.nanoTime() - startedNanos, TimeUnit.NANOSECONDS);
    }

    private Timer timer(String phase) {
        return Timer.builder("eap_order_submission_append_duration")
                .description("Order submission append duration by phase")
                .tag("phase", phase)
                .register(registry);
    }
}
