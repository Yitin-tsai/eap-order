package com.eap.eap_order.application;

import com.eap.eap_order.eventstore.CancellationPrerequisiteNotReadyException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

import static com.eap.eap_order.configuration.SchedulingConfig.ORDER_CANCELLATION_SCHEDULER;

@Component
@Slf4j
public class OrderCancellationResultReconciler {

    private final OrderCancellationResultInbox inbox;
    private final OrderEventSourcingService orderEventSourcingService;
    private final String owner = UUID.randomUUID().toString();
    private final int batchSize;
    private final long leaseMs;
    private final int maxAttempts;

    public OrderCancellationResultReconciler(
            OrderCancellationResultInbox inbox,
            OrderEventSourcingService orderEventSourcingService,
            @Value("${eap.order.cancellation-result-reconciler.batch-size:100}") int batchSize,
            @Value("${eap.order.cancellation-result-reconciler.lease-ms:30000}") long leaseMs,
            @Value("${eap.order.cancellation-result-reconciler.max-attempts:20}") int maxAttempts) {
        this.inbox = inbox;
        this.orderEventSourcingService = orderEventSourcingService;
        this.batchSize = batchSize;
        this.leaseMs = leaseMs;
        this.maxAttempts = maxAttempts;
    }

    @Scheduled(
            fixedDelayString = "${eap.order.cancellation-result-reconciler.poll-interval-ms:500}",
            initialDelayString = "${eap.order.cancellation-result-reconciler.initial-delay-ms:500}",
            scheduler = ORDER_CANCELLATION_SCHEDULER)
    public void reconcile() {
        List<OrderCancellationResultInbox.InboxEntry> entries =
                inbox.claimRetryable(batchSize, owner, leaseMs);
        for (OrderCancellationResultInbox.InboxEntry entry : entries) {
            try {
                orderEventSourcingService.applyCancellationResult(entry.event());
                if (!inbox.markApplied(entry, owner)) {
                    log.warn("Lost cancellation-result inbox lease before APPLIED: cancellationId={}",
                            entry.event().getCancellationId());
                }
            } catch (CancellationPrerequisiteNotReadyException e) {
                inbox.reschedule(entry, owner, "PENDING_PREREQUISITE", e,
                        retryDelayMs(entry.attemptCount()));
            } catch (Exception e) {
                if (entry.attemptCount() >= maxAttempts) {
                    log.error("Permanently rejecting cancellation result: cancellationId={}",
                            entry.event().getCancellationId(), e);
                    inbox.markPermanent(entry, owner, e);
                } else {
                    inbox.reschedule(entry, owner, "FAILED_RETRYABLE", e,
                            retryDelayMs(entry.attemptCount()));
                }
            }
        }
    }

    private long retryDelayMs(int attemptCount) {
        int exponent = Math.min(Math.max(attemptCount - 1, 0), 6);
        return Math.min(10_000L, 100L << exponent);
    }
}
