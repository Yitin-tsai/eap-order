package com.eap.eap_order.configuration.publishing;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class OrderPublishMetrics {

    private final Counter confirmed;
    private final Counter failed;
    private final Timer confirmDuration;

    public OrderPublishMetrics(MeterRegistry registry) {
        this.confirmed = Counter.builder("eap_order_submitted_publish_confirmed_total")
                .description("OrderSubmittedEvent publishes confirmed and routed by RabbitMQ")
                .register(registry);
        this.failed = Counter.builder("eap_order_submitted_publish_failed_total")
                .description("OrderSubmittedEvent publish attempts that failed confirmation or routing")
                .register(registry);
        this.confirmDuration = Timer.builder("eap_order_submitted_publish_confirm_duration")
                .description("Time spent publishing OrderSubmittedEvent and waiting for RabbitMQ confirmation")
                .publishPercentileHistogram()
                .register(registry);
    }

    public void confirmed() {
        confirmed.increment();
    }

    public void failed() {
        failed.increment();
    }

    public void recordDuration(Duration duration) {
        confirmDuration.record(duration);
    }
}
