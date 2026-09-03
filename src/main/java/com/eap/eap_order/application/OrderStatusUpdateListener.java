package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationSucceededEvent;
import com.eap.common.event.OrderFailedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.eap.common.constants.RabbitMQConstants.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class OrderStatusUpdateListener {

    private final OrderAssetReservationResultInbox reservationResultInbox;
    private final ObjectMapper objectMapper;
    private final OrderAssetReservationMetrics metrics;

    /**
     * 監聽 Wallet 資產保留成功的 integration event，轉成 OrderAssetReservationConfirmedV1 domain event。
     */
    @RabbitListener(
            queues = ORDER_ASSET_RESERVATION_SUCCEEDED_QUEUE,
            containerFactory = "orderAssetReservationConfirmedBatchListenerContainerFactory",
            concurrency = "${eap.order.listeners.asset-reservation-confirmed.concurrency:8}")
    public void onAssetReservationSucceeded(List<Message> messages, Channel channel) throws IOException {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        long listenerStartedAt = System.nanoTime();
        metrics.recordBatchSize(messages.size());
        List<OrderAssetReservationSucceededEvent> events;
        try {
            long deserializeStartedAt = System.nanoTime();
            events = deserializeAssetReservationSucceeded(messages);
            metrics.recordDeserialize(Duration.ofNanos(System.nanoTime() - deserializeStartedAt));
        } catch (Exception e) {
            log.error("Failed to deserialize OrderAssetReservationSucceededEvent batch: size={}", messages.size(), e);
            nack(messages, channel, false);
            metrics.recordListener(Duration.ofNanos(System.nanoTime() - listenerStartedAt));
            return;
        }

        try {
            long durableIntakeStartedAt = System.nanoTime();
            for (OrderAssetReservationSucceededEvent event : events) {
                OrderAssetReservationResultInbox.ReceiveOutcome outcome =
                        reservationResultInbox.receiveConfirmed(event);
                logReceiveOutcome(event.getOrderId(), "CONFIRMED", outcome);
            }
            metrics.recordInboxIntake(Duration.ofNanos(System.nanoTime() - durableIntakeStartedAt));
        } catch (RuntimeException e) {
            log.warn("Failed to persist OrderAssetReservationSucceededEvent batch into durable inbox: size={}", events.size(), e);
            metrics.recordListener(Duration.ofNanos(System.nanoTime() - listenerStartedAt));
            throw e;
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
        OrderAssetReservationResultInbox.ReceiveOutcome outcome =
                reservationResultInbox.receiveFailed(failedEvent);
        logReceiveOutcome(failedEvent.getOrderId(), "FAILED", outcome);
    }

    private List<OrderAssetReservationSucceededEvent> deserializeAssetReservationSucceeded(
            List<Message> messages) throws IOException {
        List<OrderAssetReservationSucceededEvent> events = new ArrayList<>(messages.size());
        for (Message message : messages) {
            events.add(objectMapper.readValue(message.getBody(), OrderAssetReservationSucceededEvent.class));
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

    private void logReceiveOutcome(
            java.util.UUID orderId,
            String resultType,
            OrderAssetReservationResultInbox.ReceiveOutcome outcome) {
        if (outcome == OrderAssetReservationResultInbox.ReceiveOutcome.CONFLICT) {
            log.error("Wallet asset reservation result conflict recorded: orderId={}, incomingType={}",
                    orderId, resultType);
            return;
        }
        log.info("Wallet asset reservation result persisted: orderId={}, type={}, outcome={}",
                orderId, resultType, outcome);
    }
}
