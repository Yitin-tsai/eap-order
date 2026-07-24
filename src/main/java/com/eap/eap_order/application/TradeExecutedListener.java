package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.eap.common.constants.RabbitMQConstants.ORDER_TRADE_EXECUTED_QUEUE;

@Component
@RequiredArgsConstructor
@Slf4j
public class TradeExecutedListener {

    private final OrderEventSourcingService orderEventSourcingService;
    private final OrderTradeExecutedInbox tradeExecutedInbox;
    private final ObjectMapper objectMapper;

    @RabbitListener(
            queues = ORDER_TRADE_EXECUTED_QUEUE,
            containerFactory = "orderTradeExecutedBatchListenerContainerFactory",
            concurrency = "${eap.order.listeners.trade-executed.concurrency:4}")
    public void handleTradeExecuted(List<Message> messages, Channel channel) throws IOException {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        List<TradeExecutedEvent> events;
        try {
            events = deserialize(messages);
        } catch (Exception e) {
            log.error("Failed to deserialize TradeExecutedEvent batch: size={}", messages.size(), e);
            nack(messages, channel, false);
            return;
        }
        log.debug("Received TradeExecutedEvent batch: size={}", events.size());

        try {
            orderEventSourcingService.applyTrades(events);
        } catch (Exception e) {
            log.warn("Failed to apply TradeExecutedEvent batch to Order state: size={}", events.size(), e);
            markFailed(events, e);
            nack(messages, channel, true);
            return;
        }
        ack(messages, channel);
    }

    private List<TradeExecutedEvent> deserialize(List<Message> messages) throws IOException {
        List<TradeExecutedEvent> events = new ArrayList<>(messages.size());
        for (Message message : messages) {
            events.add(objectMapper.readValue(message.getBody(), TradeExecutedEvent.class));
        }
        return events;
    }

    private void ack(List<Message> messages, Channel channel) throws IOException {
        channel.basicAck(lastDeliveryTag(messages), messages.size() > 1);
    }

    private void nack(List<Message> messages, Channel channel, boolean requeue) throws IOException {
        channel.basicNack(lastDeliveryTag(messages), messages.size() > 1, requeue);
    }

    private long lastDeliveryTag(List<Message> messages) {
        long last = 0L;
        for (Message message : messages) {
            last = Math.max(last, message.getMessageProperties().getDeliveryTag());
        }
        return last;
    }

    private void markFailed(List<TradeExecutedEvent> events, Exception failure) {
        try {
            tradeExecutedInbox.markFailed(events, failure);
        } catch (Exception markerFailure) {
            log.warn("Failed to mark Order TradeExecuted inbox rows as retryable failure: size={}",
                    events.size(), markerFailure);
        }
    }
}
