package com.eap.eap_order.application;

import com.eap.common.event.OrderConfirmedEvent;
import com.eap.common.event.OrderFailedEvent;
import com.eap.eap_order.controller.OrderStatusController;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.eap.common.constants.RabbitMQConstants.*;

@Component
@Slf4j
public class OrderStatusUpdateListener {

    @Autowired
    private OrderStatusController orderStatusController;

    @Autowired
    private OrderEventSourcingService orderEventSourcingService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderAssetReservationMetrics metrics;

    /**
     * 監聽 Wallet 資產保留成功的 integration event，轉成 OrderAssetReservationConfirmedV1 domain event。
     */
    @RabbitListener(
            queues = ORDER_ORDER_CONFIRMED_QUEUE,
            containerFactory = "orderAssetReservationConfirmedBatchListenerContainerFactory",
            concurrency = "${eap.order.listeners.asset-reservation-confirmed.concurrency:8}")
    public void onOrderConfirmed(List<Message> messages, Channel channel) throws IOException {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        long listenerStartedAt = System.nanoTime();
        metrics.recordBatchSize(messages.size());
        List<OrderConfirmedEvent> events;
        try {
            long deserializeStartedAt = System.nanoTime();
            events = deserializeOrderConfirmed(messages);
            metrics.recordDeserialize(Duration.ofNanos(System.nanoTime() - deserializeStartedAt));
        } catch (Exception e) {
            log.error("Failed to deserialize OrderConfirmedEvent batch: size={}", messages.size(), e);
            nack(messages, channel, false);
            metrics.recordListener(Duration.ofNanos(System.nanoTime() - listenerStartedAt));
            return;
        }

        try {
            long confirmAllStartedAt = System.nanoTime();
            orderEventSourcingService.confirmAll(events);
            metrics.recordConfirmAll(Duration.ofNanos(System.nanoTime() - confirmAllStartedAt));
            long statusUpdateStartedAt = System.nanoTime();
            for (OrderConfirmedEvent event : events) {
                log.info("收到 Wallet 資產保留成功事件，更新訂單狀態: {}", event.getOrderId());
                orderStatusController.updateOrderStatus(
                    event.getOrderId(),
                    "WALLET_CHECK_PASSED",
                    "餘額檢查通過，已進入撮合佇列"
                );
            }
            metrics.recordStatusUpdate(Duration.ofNanos(System.nanoTime() - statusUpdateStartedAt));
        } catch (Exception e) {
            log.warn("Failed to apply OrderConfirmedEvent batch to Order state: size={}", events.size(), e);
            nack(messages, channel, true);
            metrics.recordListener(Duration.ofNanos(System.nanoTime() - listenerStartedAt));
            return;
        }
        long ackStartedAt = System.nanoTime();
        try {
            ack(messages, channel);
        } finally {
            metrics.recordAck(Duration.ofNanos(System.nanoTime() - ackStartedAt));
            metrics.recordListener(Duration.ofNanos(System.nanoTime() - listenerStartedAt));
        }
    }

    /**
     * 監聽 Wallet 資產保留失敗的 integration event，轉成 OrderAssetReservationFailedV1 domain event。
     */
    @RabbitListener(
            queues = ORDER_ORDER_FAILED_QUEUE,
            concurrency = "${eap.order.listeners.asset-reservation-failed.concurrency:4}")
    public void onOrderFailed(OrderFailedEvent failedEvent) {
        log.info("收到訂單失敗通知: {} - {} ({})",
                failedEvent.getOrderId(), failedEvent.getReason(), failedEvent.getFailureType());

        String status = "INSUFFICIENT_BALANCE".equals(failedEvent.getFailureType()) ?
                       "INSUFFICIENT_BALANCE" : "FAILED";

        orderStatusController.updateOrderStatus(
            failedEvent.getOrderId(),
            status,
            failedEvent.getReason()
        );

        orderEventSourcingService.fail(failedEvent);
    }

    private List<OrderConfirmedEvent> deserializeOrderConfirmed(List<Message> messages) throws IOException {
        List<OrderConfirmedEvent> events = new ArrayList<>(messages.size());
        for (Message message : messages) {
            events.add(objectMapper.readValue(message.getBody(), OrderConfirmedEvent.class));
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
}
