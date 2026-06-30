package com.eap.eap_order.configuration.publishing;

import com.eap.common.event.OrderSubmittedEvent;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static com.eap.common.constants.RabbitMQConstants.ORDER_EXCHANGE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_SUBMITTED_KEY;

@Component
public class OrderEventPublisher {

    private final RabbitTemplate rabbitTemplate;
    private final OrderPublishMetrics metrics;
    private final long confirmTimeoutMs;

    public OrderEventPublisher(
            RabbitTemplate rabbitTemplate,
            OrderPublishMetrics metrics,
            @Value("${eap.order-publisher.confirm-timeout-ms:5000}") long confirmTimeoutMs) {
        this.rabbitTemplate = rabbitTemplate;
        this.metrics = metrics;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    public void publish(OrderSubmittedEvent event) {
        Instant startedAt = Instant.now();
        CorrelationData correlationData = new CorrelationData(event.getOrderId().toString());
        try {
            rabbitTemplate.convertAndSend(
                    ORDER_EXCHANGE,
                    ORDER_SUBMITTED_KEY,
                    event,
                    correlationData
            );
            CorrelationData.Confirm confirm = correlationData.getFuture()
                    .get(confirmTimeoutMs, TimeUnit.MILLISECONDS);
            if (!confirm.isAck()) {
                throw new AmqpException("RabbitMQ nack: " + confirm.getReason());
            }
            if (correlationData.getReturned() != null) {
                throw new AmqpException("OrderSubmittedEvent was unroutable: orderId=" + event.getOrderId());
            }
            metrics.confirmed();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.failed();
            throw new OrderPublishException("Interrupted while waiting for RabbitMQ confirmation", e);
        } catch (Exception e) {
            metrics.failed();
            throw new OrderPublishException(
                    "Order was not accepted because RabbitMQ did not confirm publication", e);
        } finally {
            metrics.recordDuration(Duration.between(startedAt, Instant.now()));
        }
    }
}
