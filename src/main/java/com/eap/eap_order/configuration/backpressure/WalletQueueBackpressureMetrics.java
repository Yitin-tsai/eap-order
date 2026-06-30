package com.eap.eap_order.configuration.backpressure;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class WalletQueueBackpressureMetrics {

    private final AtomicInteger queueDepth = new AtomicInteger(-1);
    private final AtomicInteger consumerCount = new AtomicInteger(-1);
    private final Counter hardRejected;
    private final Counter probeFailed;

    public WalletQueueBackpressureMetrics(MeterRegistry registry) {
        Gauge.builder("eap_order_wallet_queue_depth", queueDepth, AtomicInteger::get)
                .description("Cached wallet order-submitted queue depth observed by eap-order")
                .register(registry);
        Gauge.builder("eap_order_wallet_queue_consumers", consumerCount, AtomicInteger::get)
                .description("Cached wallet order-submitted consumer count observed by eap-order")
                .register(registry);
        this.hardRejected = Counter.builder("eap_order_backpressure_hard_rejected_total")
                .description("Orders rejected because the wallet queue is at hard capacity or unavailable")
                .register(registry);
        this.probeFailed = Counter.builder("eap_order_backpressure_probe_failed_total")
                .description("Failed RabbitMQ wallet queue probes")
                .register(registry);
    }

    public void queueObserved(int depth, int consumers) {
        queueDepth.set(depth);
        consumerCount.set(consumers);
    }

    public void queueUnavailable() {
        queueDepth.set(-1);
        consumerCount.set(-1);
        probeFailed.increment();
    }

    public void hardRejected() {
        hardRejected.increment();
    }
}
