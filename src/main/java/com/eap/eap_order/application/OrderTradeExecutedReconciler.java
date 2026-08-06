package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

import com.eap.eap_order.application.OrderTradeExecutedInbox.InboxEntry;

@Component
@Slf4j
public class OrderTradeExecutedReconciler {

    private final OrderTradeExecutedInbox inbox;
    private final OrderEventSourcingService orderEventSourcingService;
    private final String owner;
    private final int batchSize;
    private final long leaseMs;
    private final int maxAttempts;

    public OrderTradeExecutedReconciler(
            OrderTradeExecutedInbox inbox,
            OrderEventSourcingService orderEventSourcingService,
            @Value("${eap.order.trade-execution-reconciler.batch-size:100}") int batchSize,
            @Value("${eap.order.trade-execution-reconciler.lease-ms:30000}") long leaseMs,
            @Value("${eap.order.trade-execution-reconciler.max-attempts:20}") int maxAttempts) {
        this.inbox = inbox;
        this.orderEventSourcingService = orderEventSourcingService;
        this.owner = UUID.randomUUID().toString();
        this.batchSize = batchSize;
        this.leaseMs = leaseMs;
        this.maxAttempts = maxAttempts;
    }

    @Scheduled(
            fixedDelayString = "${eap.order.trade-execution-reconciler.poll-interval-ms:100}",
            initialDelayString = "${eap.order.trade-execution-reconciler.initial-delay-ms:500}")
    public void reconcile() {
        List<InboxEntry> pending = inbox.claimRetryable(batchSize, owner, leaseMs);
        for (InboxEntry entry : pending) {
            TradeExecutedEvent event = entry.event();
            try {
                orderEventSourcingService.applyTrades(List.of(event));
                if (!inbox.markApplied(event, owner)) {
                    log.warn("Lost TradeExecuted inbox lease before marking applied: tradeId={}", event.getTradeId());
                }
            } catch (TradeProjectionNotReadyException e) {
                inbox.reschedule(entry, owner, "PENDING_PREREQUISITE", e, retryDelayMs(entry.attemptCount()));
            } catch (TradeApplicationRejectedException e) {
                log.error("Permanently rejecting TradeExecuted event: tradeId={}", event.getTradeId(), e);
                inbox.markClaimedPermanentFailure(entry, owner, e);
            } catch (Exception e) {
                log.warn("Failed to reconcile pending TradeExecuted event: tradeId={}",
                        event.getTradeId(), e);
                if (entry.attemptCount() >= maxAttempts) {
                    inbox.markClaimedPermanentFailure(entry, owner, e);
                } else {
                    inbox.reschedule(entry, owner, "FAILED_RETRYABLE", e, retryDelayMs(entry.attemptCount()));
                }
            }
        }
    }

    private long retryDelayMs(int attemptCount) {
        int exponent = Math.min(Math.max(attemptCount - 1, 0), 6);
        return Math.min(10_000L, 100L << exponent);
    }
}
