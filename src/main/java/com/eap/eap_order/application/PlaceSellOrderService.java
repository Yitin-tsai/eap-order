package com.eap.eap_order.application;

// ...existing code...
import com.eap.eap_order.controller.dto.req.PlaceSellOrderReq;
import com.eap.eap_order.domain.entity.Order.OrderType;
import com.eap.common.event.OrderSubmittedEvent;
import com.eap.eap_order.configuration.backpressure.WalletQueueBackpressureGuard;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PlaceSellOrderService {

    @Autowired
    private OrderEventSourcingService orderEventSourcingService;

    @Autowired
    private MarketSequenceService marketSequenceService;

    @Autowired
    private WalletQueueBackpressureGuard backpressureGuard;

    @Autowired
    private OrderSubmissionMetrics metrics;

    public OrderSubmissionResult placeSellOrder(PlaceSellOrderReq request) {
        long totalStartedNanos = System.nanoTime();
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
                            .userId(request.getSeller())
                            .marketId(marketId)
                            .marketSequence(marketSequence)
                            .price(request.getSellPrice())
                            .amount(request.getAmount())
                            .orderType(OrderType.SELL.name())
                            .createdAt(LocalDateTime.now())
                            .build();
            metrics.recordBuildEvent(Duration.ofNanos(System.nanoTime() - buildEventStartedNanos));
            log.info("Creating sell order: {}", event);

            // Event Store + integration outbox are committed atomically.
            long requestStartedNanos = System.nanoTime();
            orderEventSourcingService.request(event);
            metrics.recordEventStoreRequest(Duration.ofNanos(System.nanoTime() - requestStartedNanos));
            log.info("Sell order accepted into Event Store: {}", event);

            return new OrderSubmissionResult(orderId, marketId, marketSequence);
        } finally {
            metrics.recordTotal(Duration.ofNanos(System.nanoTime() - totalStartedNanos));
        }
    }
}
