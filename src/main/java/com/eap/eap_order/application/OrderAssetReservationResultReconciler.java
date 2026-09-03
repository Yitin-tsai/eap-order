package com.eap.eap_order.application;

import com.eap.eap_order.controller.OrderStatusController;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongUnaryOperator;

@Component
@Slf4j
public class OrderAssetReservationResultReconciler {

    private final OrderAssetReservationResultInbox inbox;
    private final OrderAssetReservationResultProcessor processor;
    private final OrderAssetReservationResultErrorClassifier errorClassifier;
    private final OrderStatusController orderStatusController;
    private final String owner;
    private final int batchSize;
    private final long leaseMs;
    private final int maxAttempts;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final LongUnaryOperator jitter;

    @Autowired
    public OrderAssetReservationResultReconciler(
            OrderAssetReservationResultInbox inbox,
            OrderAssetReservationResultProcessor processor,
            OrderAssetReservationResultErrorClassifier errorClassifier,
            OrderStatusController orderStatusController,
            @Value("${eap.order.asset-reservation-result-reconciler.batch-size:100}") int batchSize,
            @Value("${eap.order.asset-reservation-result-reconciler.lease-ms:30000}") long leaseMs,
            @Value("${eap.order.asset-reservation-result-reconciler.max-attempts:20}") int maxAttempts,
            @Value("${eap.order.asset-reservation-result-reconciler.initial-backoff-ms:250}") long initialBackoffMs,
            @Value("${eap.order.asset-reservation-result-reconciler.max-backoff-ms:30000}") long maxBackoffMs) {
        this(inbox, processor, errorClassifier, orderStatusController,
                batchSize, leaseMs, maxAttempts, initialBackoffMs, maxBackoffMs,
                base -> ThreadLocalRandom.current().nextLong(Math.max(1, base / 2), base + 1));
    }

    OrderAssetReservationResultReconciler(
            OrderAssetReservationResultInbox inbox,
            OrderAssetReservationResultProcessor processor,
            OrderAssetReservationResultErrorClassifier errorClassifier,
            OrderStatusController orderStatusController,
            int batchSize,
            long leaseMs,
            int maxAttempts,
            long initialBackoffMs,
            long maxBackoffMs,
            LongUnaryOperator jitter) {
        this.inbox = inbox;
        this.processor = processor;
        this.errorClassifier = errorClassifier;
        this.orderStatusController = orderStatusController;
        this.owner = UUID.randomUUID().toString();
        this.batchSize = Math.max(1, batchSize);
        this.leaseMs = Math.max(1, leaseMs);
        this.maxAttempts = Math.max(1, maxAttempts);
        this.initialBackoffMs = Math.max(1, initialBackoffMs);
        this.maxBackoffMs = Math.max(this.initialBackoffMs, maxBackoffMs);
        this.jitter = jitter;
    }

    @Scheduled(
            fixedDelayString = "${eap.order.asset-reservation-result-reconciler.poll-interval-ms:100}",
            initialDelayString = "${eap.order.asset-reservation-result-reconciler.initial-delay-ms:500}")
    public void reconcile() {
        List<OrderAssetReservationResultInbox.InboxEntry> entries =
                inbox.claimRetryable(batchSize, owner, leaseMs);
        for (OrderAssetReservationResultInbox.InboxEntry entry : entries) {
            try {
                processor.process(entry, owner);
            } catch (Exception failure) {
                try {
                    handleFailure(entry, failure);
                } catch (Exception recordFailure) {
                    log.warn("Could not record asset reservation result failure; lease expiry will recover it: orderId={}",
                            entry.orderId(), recordFailure);
                }
                continue;
            }
            try {
                notifyRealtimeStatus(entry);
            } catch (Exception notificationFailure) {
                log.warn("Durable asset reservation result applied but realtime notification failed: orderId={}",
                        entry.orderId(), notificationFailure);
            }
        }
    }

    private void handleFailure(
            OrderAssetReservationResultInbox.InboxEntry entry,
            Exception failure) {
        OrderAssetReservationResultErrorClassifier.Classification classification =
                errorClassifier.classify(failure);
        if (classification.retryable() && entry.attemptCount() < maxAttempts) {
            long delayMs = retryDelayMs(entry.attemptCount());
            if (!inbox.reschedule(
                    entry, owner, classification.errorType(), failure, delayMs)) {
                log.warn("Lost asset reservation result inbox lease while rescheduling: orderId={}",
                        entry.orderId());
            }
            return;
        }

        String terminalType = classification.retryable()
                ? "RETRY_EXHAUSTED_" + classification.errorType()
                : classification.errorType();
        if (!inbox.markPermanent(entry, owner, terminalType, failure)) {
            log.warn("Lost asset reservation result inbox lease while marking permanent: orderId={}",
                    entry.orderId());
            return;
        }
        log.error("Asset reservation result requires intervention: orderId={}, type={}, attempts={}",
                entry.orderId(), terminalType, entry.attemptCount(), failure);
    }

    private long retryDelayMs(int attemptCount) {
        int exponent = Math.min(Math.max(attemptCount - 1, 0), 30);
        long multiplier = 1L << exponent;
        long base = initialBackoffMs > maxBackoffMs / multiplier
                ? maxBackoffMs
                : Math.min(initialBackoffMs * multiplier, maxBackoffMs);
        return Math.max(1, Math.min(maxBackoffMs, jitter.applyAsLong(base)));
    }

    private void notifyRealtimeStatus(OrderAssetReservationResultInbox.InboxEntry entry) {
        if (entry.resultType() == OrderAssetReservationResultInbox.ResultType.CONFIRMED) {
            orderStatusController.updateOrderStatus(
                    entry.orderId(), "WALLET_CHECK_PASSED", "餘額檢查通過，已進入撮合佇列");
            return;
        }
        String status = "INSUFFICIENT_BALANCE".equals(entry.failedEvent().getFailureType())
                ? "INSUFFICIENT_BALANCE"
                : "FAILED";
        orderStatusController.updateOrderStatus(
                entry.orderId(), status, entry.failedEvent().getReason());
    }
}
