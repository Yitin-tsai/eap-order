package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import static com.eap.common.constants.RabbitMQConstants.ORDER_TRADE_EXECUTED_QUEUE;

@Component
@RequiredArgsConstructor
@Slf4j
public class TradeExecutedListener {

    private final OrderEventSourcingService orderEventSourcingService;

    @RabbitListener(
            queues = ORDER_TRADE_EXECUTED_QUEUE,
            concurrency = "${eap.order.listeners.trade-executed.concurrency:4}")
    public void handleTradeExecuted(TradeExecutedEvent event) {
        log.debug("Received TradeExecutedEvent: tradeId={}, legacyMatchId={}",
                event.getTradeId(), event.getLegacyMatchId());

        orderEventSourcingService.applyTrade(event);
    }
}
