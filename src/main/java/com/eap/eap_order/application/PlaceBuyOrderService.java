package com.eap.eap_order.application;

// ...existing code...
import com.eap.eap_order.controller.dto.req.PlaceBuyOrderReq;
import com.eap.eap_order.domain.entity.Order.OrderType;
import com.eap.common.event.OrderSubmittedEvent;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.UUID;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.eap.common.constants.RabbitMQConstants.*;

@Service
@Slf4j
public class PlaceBuyOrderService {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private AuditService auditService;

    @Autowired
    private MarketSequenceService marketSequenceService;

    public OrderSubmissionResult execute(PlaceBuyOrderReq request) {
        String marketId = MarketSequenceService.DEFAULT_MARKET_ID;
        Long marketSequence = marketSequenceService.nextSequence(marketId);
        UUID orderId = UUID.randomUUID();

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
        log.info("Creating buy order: {}", event);

        // 直接發送事件，讓wallet-service異步處理
        rabbitTemplate.convertAndSend(ORDER_EXCHANGE, ORDER_SUBMITTED_KEY, event);
        log.info("Buy order create event published: {}", event);

        auditService.record("ORDER_SUBMITTED", event.getOrderId().toString(), event.getUserId(), event);

        return new OrderSubmissionResult(orderId, marketId, marketSequence);
    }
}
