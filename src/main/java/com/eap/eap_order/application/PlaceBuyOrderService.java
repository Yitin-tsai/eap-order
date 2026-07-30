package com.eap.eap_order.application;

// ...existing code...
import com.eap.eap_order.controller.dto.req.PlaceBuyOrderReq;
import com.eap.eap_order.domain.entity.Order.OrderType;
import com.eap.common.event.OrderSubmittedEvent;
import com.eap.eap_order.configuration.backpressure.WalletQueueBackpressureGuard;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PlaceBuyOrderService {

    @Autowired
    private OrderEventSourcingService orderEventSourcingService;

    @Autowired
    private MarketSequenceService marketSequenceService;

    @Autowired
    private WalletQueueBackpressureGuard backpressureGuard;

    @Autowired
    private OrderSubmissionMetrics metrics;

    public OrderSubmissionResult execute(PlaceBuyOrderReq request) {
        long totalStartedNanos = System.nanoTime();
        long preEventStoreStartedNanos = totalStartedNanos;
        try {
            long backpressureStartedNanos = System.nanoTime();
            backpressureGuard.checkCanAcceptOrder();
            metrics.recordBackpressure(Duration.ofNanos(System.nanoTime() - backpressureStartedNanos));

            String marketId = MarketSequenceService.DEFAULT_MARKET_ID;
            long sequenceStartedNanos = System.nanoTime();
            Long marketSequence = marketSequenceService.nextSequence(marketId);
            metrics.recordMarketSequence(Duration.ofNanos(System.nanoTime() - sequenceStartedNanos));

            long buildEventStartedNanos = System.nanoTime();
            UUID orderId = request.getOrderId() != null ? request.getOrderId() : UUID.randomUUID();
            OrderSubmittedEvent event =
                OrderSubmittedEvent.builder()
                    .orderId(orderId)
                    .userId(request.getBidder())
                    .marketId(marketId)
                    .marketSequence(marketSequence)
                    .price(request.getBidPrice())
                    .amount(request.getAmount())
                    .orderType(OrderType.BUY.name())
                    .createdAt(LocalDateTime.now())
                    .build();
            metrics.recordBuildEvent(Duration.ofNanos(System.nanoTime() - buildEventStartedNanos));
            log.info("Creating buy order: {}", event);

            // Event Store + integration outbox are committed atomically.
            metrics.recordPreEventStore(Duration.ofNanos(System.nanoTime() - preEventStoreStartedNanos));
            long requestStartedNanos = System.nanoTime();
            orderEventSourcingService.request(event);
            metrics.recordEventStoreRequest(Duration.ofNanos(System.nanoTime() - requestStartedNanos));
            log.info("Buy order accepted into Event Store: {}", event);

            return new OrderSubmissionResult(orderId, marketId, marketSequence);
        } finally {
            metrics.recordTotal(Duration.ofNanos(System.nanoTime() - totalStartedNanos));
        }
    }
}
