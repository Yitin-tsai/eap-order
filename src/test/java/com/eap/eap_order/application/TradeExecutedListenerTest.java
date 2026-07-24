package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
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

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class TradeExecutedListenerTest {

    @Mock
    private OrderEventSourcingService orderEventSourcingService;
    @Mock
    private OrderTradeExecutedInbox tradeExecutedInbox;
    @Mock
    private Channel channel;

    private ObjectMapper objectMapper;
    private TradeExecutedListener listener;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        listener = new TradeExecutedListener(orderEventSourcingService, tradeExecutedInbox, objectMapper);
    }

    @Test
    void handleTradeExecuted_shouldApplyAndAckWithoutInboxOnSuccess() throws Exception {
        TradeExecutedEvent event = event();
        Message message = message(event, 10L);

        listener.handleTradeExecuted(List.of(message), channel);

        verify(orderEventSourcingService).applyTrades(List.of(event));
        verifyNoInteractions(tradeExecutedInbox);
        verify(channel).basicAck(10L, false);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void handleTradeExecutedBatch_shouldApplyAndAckThroughLastDeliveryTag() throws Exception {
        TradeExecutedEvent first = event("trade-1");
        TradeExecutedEvent second = event("trade-2");
        Message firstMessage = message(first, 10L);
        Message secondMessage = message(second, 11L);

        listener.handleTradeExecuted(List.of(firstMessage, secondMessage), channel);

        verify(orderEventSourcingService).applyTrades(List.of(first, second));
        verifyNoInteractions(tradeExecutedInbox);
        verify(channel).basicAck(11L, true);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void handleTradeExecuted_whenApplyFails_shouldMarkRetryableAndRequeue() throws Exception {
        TradeExecutedEvent event = event();
        Message message = message(event, 11L);
        RuntimeException failure = new RuntimeException("apply failed");
        doThrow(failure).when(orderEventSourcingService).applyTrades(List.of(event));

        listener.handleTradeExecuted(List.of(message), channel);

        verify(orderEventSourcingService).applyTrades(List.of(event));
        verify(tradeExecutedInbox).markFailed(List.of(event), failure);
        verify(channel).basicNack(11L, false, true);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void handleTradeExecutedBatch_whenApplyFails_shouldMarkRetryableAndRequeueThroughLastDeliveryTag() throws Exception {
        TradeExecutedEvent first = event("trade-1");
        TradeExecutedEvent second = event("trade-2");
        Message firstMessage = message(first, 20L);
        Message secondMessage = message(second, 21L);
        RuntimeException failure = new RuntimeException("apply failed");
        doThrow(failure).when(orderEventSourcingService).applyTrades(List.of(first, second));

        listener.handleTradeExecuted(List.of(firstMessage, secondMessage), channel);

        verify(orderEventSourcingService).applyTrades(List.of(first, second));
        verify(tradeExecutedInbox).markFailed(List.of(first, second), failure);
        verify(channel).basicNack(21L, true, true);
        verifyNoMoreInteractions(channel);
    }

    private Message message(TradeExecutedEvent event, long deliveryTag) throws Exception {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(deliveryTag);
        return new Message(objectMapper.writeValueAsBytes(event), properties);
    }

    private TradeExecutedEvent event() {
        return event("trade-1");
    }

    private TradeExecutedEvent event(String tradeId) {
        return TradeExecutedEvent.builder()
                .tradeId(tradeId)
                .legacyMatchId(1001)
                .buyerId(UUID.randomUUID())
                .sellerId(UUID.randomUUID())
                .buyerOrderId(UUID.randomUUID())
                .sellerOrderId(UUID.randomUUID())
                .originBuyerPrice(120)
                .originSellerPrice(100)
                .dealPrice(110)
                .quantity(10)
                .occurredAt(LocalDateTime.now())
                .build();
    }
}
