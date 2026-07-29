package com.eap.eap_order.application;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class OrderAssetReservationMetrics {

    private final Timer listenerTimer;
    private final Timer deserializeTimer;
    private final Timer confirmAllTimer;
    private final Timer statusUpdateTimer;
    private final Timer ackTimer;
    private final DistributionSummary batchSizeSummary;

    public OrderAssetReservationMetrics(MeterRegistry registry) {
        this.listenerTimer = stageTimer(
                registry,
                "eap_order_asset_reservation_confirmed_listener_duration",
                "Wall-clock time spent handling a batch of OrderConfirmedEvent messages in Order service");
        this.deserializeTimer = stageTimer(
                registry,
                "eap_order_asset_reservation_confirmed_deserialize_duration",
                "Time spent deserializing OrderConfirmedEvent messages in Order service");
        this.confirmAllTimer = stageTimer(
                registry,
                "eap_order_asset_reservation_confirmed_confirm_all_duration",
                "Time spent appending OrderAssetReservationConfirmed events in Order service");
        this.statusUpdateTimer = stageTimer(
                registry,
                "eap_order_asset_reservation_confirmed_status_update_duration",
                "Time spent applying in-memory/SSE order status updates after asset reservation confirmation");
        this.ackTimer = stageTimer(
                registry,
                "eap_order_asset_reservation_confirmed_ack_duration",
                "Time spent acknowledging OrderConfirmedEvent messages to RabbitMQ");
        this.batchSizeSummary = DistributionSummary.builder("eap_order_asset_reservation_confirmed_batch_size")
                .description("Number of OrderConfirmedEvent messages per Order asset-reservation listener batch")
                .register(registry);
    }

    public void recordListener(Duration duration) {
        listenerTimer.record(duration);
    }

    public void recordBatchSize(int size) {
        batchSizeSummary.record(size);
    }

    public void recordDeserialize(Duration duration) {
        deserializeTimer.record(duration);
    }

    public void recordConfirmAll(Duration duration) {
        confirmAllTimer.record(duration);
    }

    public void recordStatusUpdate(Duration duration) {
        statusUpdateTimer.record(duration);
    }

    public void recordAck(Duration duration) {
        ackTimer.record(duration);
    }

    private Timer stageTimer(MeterRegistry registry, String name, String description) {
        return Timer.builder(name)
                .description(description)
                .publishPercentileHistogram()
                .register(registry);
    }
}
