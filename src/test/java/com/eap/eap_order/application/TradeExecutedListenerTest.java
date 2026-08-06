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
    void handleTradeExecuted_whenApplyFails_shouldPersistRetryableAndAck() throws Exception {
        TradeExecutedEvent event = event();
        Message message = message(event, 11L);
        RuntimeException failure = new RuntimeException("apply failed");
        doThrow(failure).when(orderEventSourcingService).applyTrades(List.of(event));

        listener.handleTradeExecuted(List.of(message), channel);

        verify(orderEventSourcingService).applyTrades(List.of(event));
        verify(tradeExecutedInbox).markRetryable(List.of(event), failure);
        verify(channel).basicAck(11L, false);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void handleTradeExecuted_whenProjectionIsBehind_shouldPersistPendingAndAck() throws Exception {
        TradeExecutedEvent event = event();
        Message message = message(event, 12L);
        TradeProjectionNotReadyException failure = new TradeProjectionNotReadyException("projection behind");
        doThrow(failure).when(orderEventSourcingService).applyTrades(List.of(event));

        listener.handleTradeExecuted(List.of(message), channel);

        verify(tradeExecutedInbox).markPending(List.of(event), failure);
        verify(channel).basicAck(12L, false);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void handleTradeExecutedBatch_whenApplyFails_shouldPersistRetryableAndAckThroughLastDeliveryTag() throws Exception {
        TradeExecutedEvent first = event("trade-1");
        TradeExecutedEvent second = event("trade-2");
        Message firstMessage = message(first, 20L);
        Message secondMessage = message(second, 21L);
        RuntimeException failure = new RuntimeException("apply failed");
        doThrow(failure).when(orderEventSourcingService).applyTrades(List.of(first, second));

        listener.handleTradeExecuted(List.of(firstMessage, secondMessage), channel);

        verify(orderEventSourcingService).applyTrades(List.of(first, second));
        verify(tradeExecutedInbox).markRetryable(List.of(first, second), failure);
        verify(channel).basicAck(21L, true);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void handleTradeExecuted_whenOrderStateRejectsTrade_shouldPersistPermanentFailureAndAck() throws Exception {
        TradeExecutedEvent event = event();
        Message message = message(event, 22L);
        TradeApplicationRejectedException failure = new TradeApplicationRejectedException("invalid state");
        doThrow(failure).when(orderEventSourcingService).applyTrades(List.of(event));
        doThrow(failure).when(orderEventSourcingService).applyTrade(event);

        listener.handleTradeExecuted(List.of(message), channel);

        verify(orderEventSourcingService).applyTrades(List.of(event));
        verify(orderEventSourcingService).applyTrade(event);
        verify(tradeExecutedInbox).markPermanentFailure(List.of(event), failure);
        verify(channel).basicAck(22L, false);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void handleTradeExecutedBatch_whenOneTradeIsRejected_shouldIsolateFailureAndApplyRemainingEvents() throws Exception {
        TradeExecutedEvent first = event("trade-1");
        TradeExecutedEvent rejected = event("trade-2");
        TradeExecutedEvent last = event("trade-3");
        List<TradeExecutedEvent> batch = List.of(first, rejected, last);
        TradeApplicationRejectedException failure = new TradeApplicationRejectedException("invalid state");
        doThrow(failure).when(orderEventSourcingService).applyTrades(batch);
        doThrow(failure).when(orderEventSourcingService).applyTrade(rejected);

        listener.handleTradeExecuted(List.of(
                message(first, 30L), message(rejected, 31L), message(last, 32L)), channel);

        verify(orderEventSourcingService).applyTrade(first);
        verify(orderEventSourcingService).applyTrade(rejected);
        verify(orderEventSourcingService).applyTrade(last);
        verify(tradeExecutedInbox).markPermanentFailure(List.of(rejected), failure);
        verify(channel).basicAck(32L, true);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void handleTradeExecutedBatch_whenIsolatedFailureCannotBePersisted_shouldRequeueBatch() throws Exception {
        TradeExecutedEvent rejected = event("trade-1");
        TradeExecutedEvent next = event("trade-2");
        List<TradeExecutedEvent> batch = List.of(rejected, next);
        TradeApplicationRejectedException failure = new TradeApplicationRejectedException("invalid state");
        doThrow(failure).when(orderEventSourcingService).applyTrades(batch);
        doThrow(failure).when(orderEventSourcingService).applyTrade(rejected);
        doThrow(new RuntimeException("inbox unavailable"))
                .when(tradeExecutedInbox).markPermanentFailure(List.of(rejected), failure);

        listener.handleTradeExecuted(List.of(message(rejected, 40L), message(next, 41L)), channel);

        verify(orderEventSourcingService).applyTrade(rejected);
        verify(channel).basicNack(41L, true, true);
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
