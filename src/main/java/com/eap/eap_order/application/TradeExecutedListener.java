package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.eap.common.constants.RabbitMQConstants.ORDER_TRADE_EXECUTED_QUEUE;

@Component
@RequiredArgsConstructor
@Slf4j
public class TradeExecutedListener {

    private final OrderEventSourcingService orderEventSourcingService;

    @RabbitListener(
            queues = ORDER_TRADE_EXECUTED_QUEUE,
            containerFactory = "orderTradeExecutedBatchListenerContainerFactory",
            concurrency = "${eap.order.listeners.trade-executed.concurrency:4}")
    public void handleTradeExecuted(List<TradeExecutedEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        log.debug("Received TradeExecutedEvent batch: size={}", events.size());

        orderEventSourcingService.applyTrades(events);
    }
}
