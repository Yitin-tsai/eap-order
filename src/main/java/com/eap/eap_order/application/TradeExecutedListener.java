package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
import com.eap.eap_order.configuration.repository.OrderExecutionLinkRepository;
import com.eap.eap_order.domain.entity.OrderExecutionLinkEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static com.eap.common.constants.RabbitMQConstants.ORDER_TRADE_EXECUTED_QUEUE;

@Component
@RequiredArgsConstructor
@Slf4j
public class TradeExecutedListener {

    private final OrderExecutionLinkRepository orderExecutionLinkRepository;
    private final OrderEventSourcingService orderEventSourcingService;

    @RabbitListener(
            queues = ORDER_TRADE_EXECUTED_QUEUE,
            concurrency = "${eap.order.listeners.trade-executed.concurrency:4}")
    @Transactional
    public void handleTradeExecuted(TradeExecutedEvent event) {
        log.debug("Received TradeExecutedEvent: tradeId={}, legacyMatchId={}",
                event.getTradeId(), event.getLegacyMatchId());

        applyForOrder(event, event.getBuyerOrderId(), "BUY");
        applyForOrder(event, event.getSellerOrderId(), "SELL");
    }

    private void applyForOrder(TradeExecutedEvent event, UUID orderId, String side) {
        if (orderId == null) {
            return;
        }
        if (orderExecutionLinkRepository.existsByTradeIdAndOrderId(event.getTradeId(), orderId)) {
            log.debug("Trade already applied to order, skipping duplicate: tradeId={}, orderId={}",
                    event.getTradeId(), orderId);
            return;
        }

        orderEventSourcingService.match(orderId, event);
        orderExecutionLinkRepository.save(new OrderExecutionLinkEntity(
                event.getTradeId(),
                orderId,
                side,
                event.getDealPrice(),
                event.getQuantity(),
                event.getOccurredAt()));
    }
}
