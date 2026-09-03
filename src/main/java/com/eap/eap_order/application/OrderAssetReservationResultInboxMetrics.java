package com.eap.eap_order.application;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class OrderAssetReservationResultInboxMetrics {

    private static final List<String> STATUSES = List.of(
            "PENDING",
            "FAILED_RETRYABLE",
            "IN_PROGRESS",
            "FAILED_PERMANENT");

    private final OrderAssetReservationResultInbox inbox;
    private final MultiGauge statusRows;
    private final MultiGauge incidentRows;

    public OrderAssetReservationResultInboxMetrics(
            OrderAssetReservationResultInbox inbox,
            MeterRegistry registry) {
        this.inbox = inbox;
        this.statusRows = MultiGauge.builder("eap_order_asset_reservation_result_inbox_rows")
                .description("Current Wallet asset-reservation-result inbox rows by actionable status")
                .register(registry);
        this.incidentRows = MultiGauge.builder("eap_order_asset_reservation_result_incident_rows")
                .description("Current Wallet asset-reservation-result consistency incidents")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${eap.order.asset-reservation-result-inbox-metrics.poll-interval-ms:10000}")
    public void refresh() {
        Map<String, Long> counts = inbox.countByStatus();
        statusRows.register(
                STATUSES.stream()
                        .map(status -> MultiGauge.Row.of(
                                Tags.of("status", status), counts.getOrDefault(status, 0L)))
                        .toList(),
                true);
        incidentRows.register(
                List.of(MultiGauge.Row.of(Tags.of("type", "IDENTITY_CONFLICT"), inbox.countConflicts())),
                true);
    }
}
