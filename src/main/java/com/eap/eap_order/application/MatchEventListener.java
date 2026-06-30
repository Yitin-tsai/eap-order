package com.eap.eap_order.application;

import com.eap.eap_order.configuration.repository.MathedOrderRepository;
import com.eap.eap_order.domain.entity.MatchOrderEntity;
import com.eap.common.event.OrderMatchedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.eap.common.constants.RabbitMQConstants.*;

@Component
@Slf4j
public class MatchEventListener {

    @Autowired
    private MathedOrderRepository matchOrderRepository;

    @Autowired
    private MarketDataService marketDataService;

    @Autowired
    private OrderEventSourcingService orderEventSourcingService;

    /**
     * Handles order matched events for this module
     * - Performs idempotency check using matchId
     * - Saves match record to database
     * - Pushes real-time trade data to WebSocket clients
     * - Updates market statistics
     *
     * Idempotency: If the same matchId is received multiple times (due to RabbitMQ retry,
     * network issues, etc.), only the first event will be processed.
     */
    @RabbitListener(
            queues = ORDER_ORDER_MATCHED_QUEUE,
            concurrency = "${eap.order.listeners.order-matched.concurrency:4}")
    @org.springframework.transaction.annotation.Transactional
    public void handleOrderMatched(OrderMatchedEvent event) {
        log.debug("Received OrderMatchedEvent: {}", event);

        // Idempotency check: verify if this match has already been processed
        if (event.getMatchId() != null && matchOrderRepository.existsByMatchId(event.getMatchId())) {
            log.debug("Match already processed, skipping duplicate: matchId={}", event.getMatchId());
            return;
        }

        MatchOrderEntity matchOrder =
                MatchOrderEntity.builder()
                        .matchId(event.getMatchId())
                        .buyerUuid(event.getBuyerId())
                        .sellerUuid(event.getSellerId())
                        .price(event.getDealPrice())
                        .amount(event.getAmount())
                        .updateTime(event.getMatchedAt())
                        .orderType(event.getOrderType())
                        .build();

        // 保存成交記錄
        MatchOrderEntity savedOrder = matchOrderRepository.save(matchOrder);

        if (event.getBuyerOrderId() != null) {
            orderEventSourcingService.match(event.getBuyerOrderId(), event);
        }
        if (event.getSellerOrderId() != null) {
            orderEventSourcingService.match(event.getSellerOrderId(), event);
        }

        // 推送實時成交數據到 WebSocket
        marketDataService.pushRealtimeTrade(savedOrder);

        // 推送更新後的市場統計數據
        marketDataService.pushMarketData();

    }
}
