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
    private final Timer outboxSelectDuration;
    private final Timer outboxPublishEnqueueDuration;
    private final Timer outboxConfirmDuration;
    private final Timer outboxMarkSentDuration;
    private final Timer outboxBatchDuration;

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
        this.outboxSelectDuration = stageTimer(
                registry,
                "eap_order_outbox_select_duration",
                "Time spent selecting pending order outbox records");
        this.outboxPublishEnqueueDuration = stageTimer(
                registry,
                "eap_order_outbox_publish_enqueue_duration",
                "Time spent building and enqueueing order outbox messages to RabbitMQ");
        this.outboxConfirmDuration = stageTimer(
                registry,
                "eap_order_outbox_confirm_duration",
                "Time spent waiting for RabbitMQ publisher confirms for order outbox records");
        this.outboxMarkSentDuration = stageTimer(
                registry,
                "eap_order_outbox_mark_sent_duration",
                "Time spent marking confirmed order outbox records as SENT");
        this.outboxBatchDuration = stageTimer(
                registry,
                "eap_order_outbox_batch_duration",
                "Wall-clock time spent processing one order outbox relay batch");
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

    public void recordOutboxSelect(Duration duration) {
        outboxSelectDuration.record(duration);
    }

    public void recordOutboxPublishEnqueue(Duration duration) {
        outboxPublishEnqueueDuration.record(duration);
    }

    public void recordOutboxConfirm(Duration duration) {
        outboxConfirmDuration.record(duration);
    }

    public void recordOutboxMarkSent(Duration duration) {
        outboxMarkSentDuration.record(duration);
    }

    public void recordOutboxBatch(Duration duration) {
        outboxBatchDuration.record(duration);
    }

    private Timer stageTimer(MeterRegistry registry, String name, String description) {
        return Timer.builder(name)
                .description(description)
                .publishPercentileHistogram()
                .register(registry);
    }
}
