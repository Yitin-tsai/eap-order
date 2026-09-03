package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationSucceededEvent;
import com.eap.common.event.OrderFailedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderStatusUpdateListenerTest {

    @Mock
    private OrderAssetReservationResultInbox inbox;
    @Mock
    private OrderAssetReservationMetrics metrics;
    @Mock
    private Channel channel;

    private ObjectMapper objectMapper;
    private OrderStatusUpdateListener listener;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        listener = new OrderStatusUpdateListener(inbox, objectMapper, metrics);
    }

    @Test
    void confirmed_shouldPersistBeforeAcknowledging() throws Exception {
        OrderAssetReservationSucceededEvent event = confirmed();
        when(inbox.receiveConfirmed(event))
                .thenReturn(OrderAssetReservationResultInbox.ReceiveOutcome.ACCEPTED);

        listener.onAssetReservationSucceeded(List.of(message(event, 11L)), channel);

        verify(inbox).receiveConfirmed(event);
        verify(channel).basicAck(11L, false);
        verify(channel, never()).basicNack(11L, false, true);
    }

    @Test
    void confirmed_whenInboxIsUnavailable_shouldNotAcknowledge() throws Exception {
        OrderAssetReservationSucceededEvent event = confirmed();
        RuntimeException failure = new RuntimeException("database unavailable");
        when(inbox.receiveConfirmed(event)).thenThrow(failure);

        assertThatThrownBy(() -> listener.onAssetReservationSucceeded(List.of(message(event, 12L)), channel))
                .isSameAs(failure);

        verify(channel, never()).basicAck(12L, false);
        verify(channel, never()).basicNack(12L, false, true);
    }

    @Test
    void confirmedConflict_shouldAcknowledgeAfterIncidentIsDurable() throws Exception {
        OrderAssetReservationSucceededEvent event = confirmed();
        when(inbox.receiveConfirmed(event))
                .thenReturn(OrderAssetReservationResultInbox.ReceiveOutcome.CONFLICT);

        listener.onAssetReservationSucceeded(List.of(message(event, 13L)), channel);

        verify(channel).basicAck(13L, false);
    }

    @Test
    void malformedConfirmed_shouldRejectWithoutRequeue() throws Exception {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(14L);

        listener.onAssetReservationSucceeded(List.of(new Message("not-json".getBytes(), properties)), channel);

        verify(channel).basicNack(14L, false, false);
        verify(channel, never()).basicAck(14L, false);
    }

    @Test
    void failed_shouldUseTheSameDurableInbox() {
        OrderFailedEvent event = failed();
        when(inbox.receiveFailed(event))
                .thenReturn(OrderAssetReservationResultInbox.ReceiveOutcome.ACCEPTED);

        listener.onOrderFailed(event);

        verify(inbox).receiveFailed(event);
    }

    private Message message(OrderAssetReservationSucceededEvent event, long deliveryTag) throws Exception {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(deliveryTag);
        return new Message(objectMapper.writeValueAsBytes(event), properties);
    }

    private OrderAssetReservationSucceededEvent confirmed() {
        return OrderAssetReservationSucceededEvent.builder()
                .orderId(UUID.randomUUID())
                .userId(UUID.randomUUID())
                .marketId("ENERGY-SPOT")
                .marketSequence(1L)
                .price(100)
                .amount(3)
                .orderType("BUY")
                .createdAt(LocalDateTime.of(2026, 8, 31, 10, 0))
                .build();
    }

    private OrderFailedEvent failed() {
        return OrderFailedEvent.builder()
                .orderId(UUID.randomUUID())
                .userId(UUID.randomUUID())
                .reason("餘額不足")
                .failureType("INSUFFICIENT_BALANCE")
                .failedAt(LocalDateTime.of(2026, 8, 31, 10, 0))
                .build();
    }
}
