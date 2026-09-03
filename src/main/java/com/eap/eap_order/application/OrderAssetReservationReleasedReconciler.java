package com.eap.eap_order.application;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.eap.eap_order.configuration.SchedulingConfig.ORDER_CANCELLATION_SCHEDULER;

@Component
@Slf4j
public class OrderAssetReservationReleasedReconciler {

    private final OrderAssetReservationReleasedInbox inbox;
    private final OrderAssetReservationReleasedProcessor processor;
    private final OrderCancellationCompletionErrorClassifier classifier;
    private final String owner = UUID.randomUUID().toString();
    private final int batchSize;
    private final long leaseMs;
    private final int maxAttempts;

    public OrderAssetReservationReleasedReconciler(
            OrderAssetReservationReleasedInbox inbox,
            OrderAssetReservationReleasedProcessor processor,
            OrderCancellationCompletionErrorClassifier classifier,
            @Value("${eap.order.asset-release-reconciler.batch-size:100}") int batchSize,
            @Value("${eap.order.asset-release-reconciler.lease-ms:30000}") long leaseMs,
            @Value("${eap.order.asset-release-reconciler.max-attempts:20}") int maxAttempts) {
        this.inbox = inbox;
        this.processor = processor;
        this.classifier = classifier;
        this.batchSize = Math.max(1, batchSize);
        this.leaseMs = Math.max(1, leaseMs);
        this.maxAttempts = Math.max(1, maxAttempts);
    }

    @Scheduled(
            fixedDelayString = "${eap.order.asset-release-reconciler.poll-interval-ms:100}",
            initialDelayString = "${eap.order.asset-release-reconciler.initial-delay-ms:500}",
            scheduler = ORDER_CANCELLATION_SCHEDULER)
    public void reconcile() {
        List<OrderAssetReservationReleasedInbox.InboxEntry> entries =
                inbox.claimRetryable(batchSize, owner, leaseMs);
        for (OrderAssetReservationReleasedInbox.InboxEntry entry : entries) {
            process(entry);
        }
    }

    private void process(OrderAssetReservationReleasedInbox.InboxEntry entry) {
        try {
            processor.process(entry, owner);
        } catch (Exception failure) {
            OrderCancellationCompletionErrorClassifier.Classification classification = classifier.classify(failure);
            if (!classification.retryable()
                    || (!classification.prerequisite() && entry.attemptCount() >= maxAttempts)) {
                inbox.markPermanent(entry, owner, classification.errorType(), failure);
                log.error("Order cancellation completion permanently failed: cancellationId={}, attempts={}",
                        entry.event().getCancellationId(), entry.attemptCount(), failure);
                return;
            }
            String status = classification.prerequisite()
                    ? "PENDING_PREREQUISITE"
                    : "FAILED_RETRYABLE";
            long delayMs = retryDelayWithJitter(entry.attemptCount());
            inbox.reschedule(entry, owner, status, classification.errorType(), failure, delayMs);
        }
    }

    private long retryDelayWithJitter(int attemptCount) {
        int exponent = Math.min(Math.max(attemptCount - 1, 0), 7);
        long base = Math.min(30_000L, 250L << exponent);
        long jitter = ThreadLocalRandom.current().nextLong(Math.max(1L, base / 4));
        return Math.min(30_000L, base + jitter);
    }
}
