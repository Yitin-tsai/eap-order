package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TradeExecutedListenerTest {

    @Mock
    private OrderEventSourcingService orderEventSourcingService;

    @Test
    void handleTradeExecuted_shouldDispatchBuyerAndSellerOrders() {
        TradeExecutedEvent event = event();
        TradeExecutedListener listener = new TradeExecutedListener(orderEventSourcingService);

        listener.handleTradeExecuted(event);

        verify(orderEventSourcingService).match(event.getBuyerOrderId(), event, "BUY");
        verify(orderEventSourcingService).match(event.getSellerOrderId(), event, "SELL");
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
