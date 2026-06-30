package com.eap.eap_order.configuration.publishing;

import com.eap.common.event.OrderSubmittedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OrderEventPublisherTest {

    @Mock
    private RabbitTemplate rabbitTemplate;

    @Mock
    private OrderPublishMetrics metrics;

    private OrderEventPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new OrderEventPublisher(rabbitTemplate, metrics, 1000);
    }

    @Test
    void brokerAck_shouldCompleteSuccessfully() {
        doAnswer(invocation -> {
            CorrelationData correlationData = invocation.getArgument(3);
            correlationData.getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), (Object) any(), any(CorrelationData.class));

        assertDoesNotThrow(() -> publisher.publish(event()));

        verify(metrics).confirmed();
        verify(metrics, never()).failed();
        verify(metrics).recordDuration(any(Duration.class));
    }

    @Test
    void brokerNack_shouldRejectPublication() {
        doAnswer(invocation -> {
            CorrelationData correlationData = invocation.getArgument(3);
            correlationData.getFuture().complete(new CorrelationData.Confirm(false, "rejected"));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), (Object) any(), any(CorrelationData.class));

        OrderPublishException exception = assertThrows(OrderPublishException.class, () -> publisher.publish(event()));

        verify(metrics).failed();
        verify(metrics, never()).confirmed();
        verify(metrics).recordDuration(any(Duration.class));
        assertInstanceOf(AmqpException.class, exception.getCause());
    }

    @Test
    void brokerAckButUnroutable_shouldRejectPublication() {
        doAnswer(invocation -> {
            CorrelationData correlationData = invocation.getArgument(3);
            correlationData.setReturned(new ReturnedMessage(
                    new Message(new byte[0]),
                    312,
                    "NO_ROUTE",
                    "order.exchange",
                    "missing.routing.key"
            ));
            correlationData.getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), (Object) any(), any(CorrelationData.class));

        OrderPublishException exception = assertThrows(OrderPublishException.class, () -> publisher.publish(event()));

        verify(metrics).failed();
        verify(metrics, never()).confirmed();
        verify(metrics).recordDuration(any(Duration.class));
        assertInstanceOf(AmqpException.class, exception.getCause());
    }

    @Test
    void confirmTimeout_shouldRejectPublication() {
        publisher = new OrderEventPublisher(rabbitTemplate, metrics, 20);

        OrderPublishException exception = assertThrows(OrderPublishException.class, () -> publisher.publish(event()));

        verify(metrics).failed();
        verify(metrics, never()).confirmed();
        verify(metrics).recordDuration(any(Duration.class));
        assertInstanceOf(TimeoutException.class, exception.getCause());
    }

    @Test
    void synchronousPublishException_shouldRejectPublication() {
        doThrow(new AmqpException("connection refused"))
                .when(rabbitTemplate)
                .convertAndSend(anyString(), anyString(), (Object) any(), any(CorrelationData.class));

        OrderPublishException exception = assertThrows(OrderPublishException.class, () -> publisher.publish(event()));

        verify(metrics, times(1)).failed();
        verify(metrics, never()).confirmed();
        verify(metrics).recordDuration(any(Duration.class));
        assertInstanceOf(AmqpException.class, exception.getCause());
    }

    private OrderSubmittedEvent event() {
        return OrderSubmittedEvent.builder()
                .orderId(UUID.randomUUID())
                .userId(UUID.randomUUID())
                .build();
    }
}
