package com.eap.eap_order.configuration.publishing;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
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
    private final Timer outboxPublishStageDuration;
    private final Timer outboxPublishEnqueueDuration;
    private final Timer outboxConfirmDuration;
    private final Timer outboxFirstConfirmDuration;
    private final Timer outboxRemainingConfirmDuration;
    private final Timer outboxConfirmWallDuration;
    private final Timer outboxPostConfirmMarkGapDuration;
    private final Timer outboxMarkSentDuration;
    private final Timer outboxBatchDuration;
    private final DistributionSummary outboxBatchSize;
    private final DistributionSummary outboxConfirmedBatchSize;

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
        this.outboxPublishStageDuration = stageTimer(
                registry,
                "eap_order_outbox_publish_stage_duration",
                "Wall-clock time spent in the order outbox publish stage before mark-SENT");
        this.outboxPublishEnqueueDuration = stageTimer(
                registry,
                "eap_order_outbox_publish_enqueue_duration",
                "Time spent building and enqueueing order outbox messages to RabbitMQ");
        this.outboxConfirmDuration = stageTimer(
                registry,
                "eap_order_outbox_confirm_duration",
                "Time spent waiting for RabbitMQ publisher confirms for order outbox records");
        this.outboxFirstConfirmDuration = stageTimer(
                registry,
                "eap_order_outbox_first_confirm_duration",
                "Time spent waiting for the first RabbitMQ publisher confirm in an order outbox chunk");
        this.outboxRemainingConfirmDuration = stageTimer(
                registry,
                "eap_order_outbox_remaining_confirm_duration",
                "Time spent waiting for RabbitMQ publisher confirms after the first confirmed order outbox record in a chunk");
        this.outboxConfirmWallDuration = stageTimer(
                registry,
                "eap_order_outbox_confirm_wall_duration",
                "Batch or chunk wall-clock time spent waiting for RabbitMQ publisher confirms for order outbox records");
        this.outboxPostConfirmMarkGapDuration = stageTimer(
                registry,
                "eap_order_outbox_post_confirm_mark_gap_duration",
                "Wall-clock gap between completing order outbox publisher confirms and starting mark-SENT");
        this.outboxMarkSentDuration = stageTimer(
                registry,
                "eap_order_outbox_mark_sent_duration",
                "Time spent marking confirmed order outbox records as SENT");
        this.outboxBatchDuration = stageTimer(
                registry,
                "eap_order_outbox_batch_duration",
                "Wall-clock time spent processing one order outbox relay batch");
        this.outboxBatchSize = DistributionSummary.builder("eap_order_outbox_batch_size")
                .description("Number of order outbox records selected per relay batch")
                .register(registry);
        this.outboxConfirmedBatchSize = DistributionSummary.builder("eap_order_outbox_confirmed_batch_size")
                .description("Number of order outbox records marked SENT per relay batch")
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

    public void recordOutboxSelect(Duration duration) {
        outboxSelectDuration.record(duration);
    }

    public void recordOutboxPublishStage(Duration duration) {
        outboxPublishStageDuration.record(duration);
    }

    public void recordOutboxPublishEnqueue(Duration duration) {
        outboxPublishEnqueueDuration.record(duration);
    }

    public void recordOutboxConfirm(Duration duration) {
        outboxConfirmDuration.record(duration);
    }

    public void recordOutboxFirstConfirm(Duration duration) {
        outboxFirstConfirmDuration.record(duration);
    }

    public void recordOutboxRemainingConfirm(Duration duration) {
        outboxRemainingConfirmDuration.record(duration);
    }

    public void recordOutboxConfirmWall(Duration duration) {
        outboxConfirmWallDuration.record(duration);
    }

    public void recordOutboxPostConfirmMarkGap(Duration duration) {
        outboxPostConfirmMarkGapDuration.record(duration);
    }

    public void recordOutboxMarkSent(Duration duration) {
        outboxMarkSentDuration.record(duration);
    }

    public void recordOutboxBatch(Duration duration) {
        outboxBatchDuration.record(duration);
    }

    public void recordOutboxBatchSize(int size) {
        outboxBatchSize.record(size);
    }

    public void recordOutboxConfirmedBatchSize(int size) {
        outboxConfirmedBatchSize.record(size);
    }

    private Timer stageTimer(MeterRegistry registry, String name, String description) {
        return Timer.builder(name)
                .description(description)
                .publishPercentileHistogram()
                .register(registry);
    }
}
