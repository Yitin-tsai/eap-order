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
        } catch (TradeProjectionNotReadyException e) {
            log.debug("Order projection is behind TradeExecuted; deferring batch: size={}", events.size());
            if (markPending(events, e)) {
                ack(messages, channel);
            } else {
                nack(messages, channel, true);
            }
            return;
        } catch (TradeApplicationRejectedException e) {
            log.error("TradeExecutedEvent batch contains an event that contradicts Order state: size={}",
                    events.size(), e);
            if (isolateRejectedEvents(events)) {
                ack(messages, channel);
            } else {
                nack(messages, channel, true);
            }
            return;
        } catch (Exception e) {
            log.warn("Failed to apply TradeExecutedEvent batch to Order state: size={}", events.size(), e);
            if (markRetryable(events, e)) {
                ack(messages, channel);
            } else {
                nack(messages, channel, true);
            }
            return;
        }
        ack(messages, channel);
    }

    /**
     * A batch append can fall back to individual application and stop at the first invalid trade.
     * Replaying each event is safe because trade application is idempotent, and prevents one bad
     * event from permanently rejecting unrelated events later in the same broker batch.
     */
    private boolean isolateRejectedEvents(List<TradeExecutedEvent> events) {
        for (TradeExecutedEvent event : events) {
            try {
                orderEventSourcingService.applyTrade(event);
            } catch (TradeProjectionNotReadyException failure) {
                if (!markPending(List.of(event), failure)) {
                    return false;
                }
            } catch (TradeApplicationRejectedException failure) {
                log.error("TradeExecutedEvent contradicts Order state: tradeId={}", event.getTradeId(), failure);
                if (!markPermanent(List.of(event), failure)) {
                    return false;
                }
            } catch (Exception failure) {
                if (!markRetryable(List.of(event), failure)) {
                    return false;
                }
            }
        }
        return true;
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

    private boolean markRetryable(List<TradeExecutedEvent> events, Exception failure) {
        try {
            tradeExecutedInbox.markRetryable(events, failure);
            return true;
        } catch (Exception markerFailure) {
            log.warn("Failed to mark Order TradeExecuted inbox rows as retryable failure: size={}",
                    events.size(), markerFailure);
            return false;
        }
    }

    private boolean markPermanent(List<TradeExecutedEvent> events, Exception failure) {
        try {
            tradeExecutedInbox.markPermanentFailure(events, failure);
            return true;
        } catch (Exception markerFailure) {
            log.warn("Failed to persist permanently rejected Order TradeExecuted rows: size={}",
                    events.size(), markerFailure);
            return false;
        }
    }

    private boolean markPending(List<TradeExecutedEvent> events, Exception failure) {
        try {
            tradeExecutedInbox.markPending(events, failure);
            return true;
        } catch (Exception markerFailure) {
            log.warn("Failed to persist pending Order TradeExecuted inbox rows: size={}",
                    events.size(), markerFailure);
            return false;
        }
    }
}
