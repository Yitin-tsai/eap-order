package com.eap.eap_order.application;

// ...existing code...
import com.eap.eap_order.controller.dto.req.PlaceSellOrderReq;
import com.eap.eap_order.domain.entity.Order.OrderType;
import com.eap.common.event.OrderSubmittedEvent;

import java.time.LocalDateTime;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.eap.common.constants.RabbitMQConstants.*;

@Service
@Slf4j
public class PlaceSellOrderService {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private AuditService auditService;

    public UUID placeSellOrder(PlaceSellOrderReq request) {

        OrderSubmittedEvent event =
                OrderSubmittedEvent.builder()
                        .orderId(UUID.randomUUID())
                        .userId(request.getSeller())
                        .price(request.getSellPrice())
                        .amount(request.getAmount())
                        .orderType(OrderType.SELL.name())
                        .createdAt(LocalDateTime.now())
                        .build();
        log.info("Creating sell order: {}", event);

        // 直接發送事件，讓wallet-service異步處理
        rabbitTemplate.convertAndSend(ORDER_EXCHANGE, ORDER_SUBMITTED_KEY, event);
        log.info("Sell order create event published: {}", event);

        auditService.record("ORDER_SUBMITTED", event.getOrderId().toString(), event.getUserId(), event);

        return event.getOrderId();
    }
}
