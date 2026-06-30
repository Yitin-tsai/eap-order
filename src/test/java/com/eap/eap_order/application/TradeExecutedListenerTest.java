package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
import com.eap.eap_order.configuration.repository.OrderExecutionLinkRepository;
import com.eap.eap_order.domain.entity.OrderExecutionLinkEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
class TradeExecutedListenerTest {

    @Mock
    private OrderExecutionLinkRepository orderExecutionLinkRepository;

    @Mock
    private OrderEventSourcingService orderEventSourcingService;

    @Test
    void handleTradeExecuted_shouldApplyBuyerAndSellerOnce() {
        TradeExecutedEvent event = event();
        TradeExecutedListener listener = new TradeExecutedListener(
                orderExecutionLinkRepository, orderEventSourcingService);

        listener.handleTradeExecuted(event);

        verify(orderEventSourcingService).match(event.getBuyerOrderId(), event, "BUY");
        verify(orderEventSourcingService).match(event.getSellerOrderId(), event, "SELL");

        ArgumentCaptor<OrderExecutionLinkEntity> captor =
                ArgumentCaptor.forClass(OrderExecutionLinkEntity.class);
        verify(orderExecutionLinkRepository, times(2)).save(captor.capture());
        assertEquals("BUY", captor.getAllValues().get(0).getSide());
        assertEquals("SELL", captor.getAllValues().get(1).getSide());
    }

    @Test
    void handleTradeExecuted_duplicateTradeOrderLink_shouldSkipThatOrder() {
        TradeExecutedEvent event = event();
        when(orderExecutionLinkRepository.existsByTradeIdAndOrderId(
                event.getTradeId(), event.getBuyerOrderId())).thenReturn(true);
        TradeExecutedListener listener = new TradeExecutedListener(
                orderExecutionLinkRepository, orderEventSourcingService);

        listener.handleTradeExecuted(event);

        verify(orderEventSourcingService, never()).match(event.getBuyerOrderId(), event, "BUY");
        verify(orderEventSourcingService).match(event.getSellerOrderId(), event, "SELL");
        verify(orderExecutionLinkRepository, times(1)).save(any(OrderExecutionLinkEntity.class));
    }

    private TradeExecutedEvent event() {
        return TradeExecutedEvent.builder()
                .tradeId("trade-1")
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
