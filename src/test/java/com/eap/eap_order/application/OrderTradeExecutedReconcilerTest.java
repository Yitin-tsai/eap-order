package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderTradeExecutedReconcilerTest {

    @Mock
    private OrderTradeExecutedInbox inbox;
    @Mock
    private OrderEventSourcingService orderEventSourcingService;

    @Test
    void reconcile_shouldContinueAfterOneTradeIsStillWaitingForPrerequisite() {
        TradeExecutedEvent waiting = event("waiting");
        TradeExecutedEvent ready = event("ready");
        var waitingEntry = new OrderTradeExecutedInbox.InboxEntry(waiting, 2);
        var readyEntry = new OrderTradeExecutedInbox.InboxEntry(ready, 2);
        when(inbox.claimRetryable(eq(100), anyString(), eq(30_000L)))
                .thenReturn(List.of(waitingEntry, readyEntry));
        TradeProjectionNotReadyException lag = new TradeProjectionNotReadyException("lag");
        doThrow(lag).when(orderEventSourcingService).applyTrades(List.of(waiting));
        when(inbox.markApplied(eq(ready), anyString())).thenReturn(true);

        reconciler().reconcile();

        verify(inbox).reschedule(eq(waitingEntry), anyString(),
                eq("PENDING_PREREQUISITE"), eq(lag), anyLong());
        verify(orderEventSourcingService).applyTrades(List.of(ready));
        verify(inbox).markApplied(eq(ready), anyString());
    }

    @Test
    void reconcile_shouldStopRetryingAfterMaximumAttempts() {
        TradeExecutedEvent event = event("failed");
        var entry = new OrderTradeExecutedInbox.InboxEntry(event, 20);
        when(inbox.claimRetryable(eq(100), anyString(), eq(30_000L)))
                .thenReturn(List.of(entry));
        RuntimeException failure = new RuntimeException("db unavailable");
        doThrow(failure).when(orderEventSourcingService).applyTrades(List.of(event));

        reconciler().reconcile();

        verify(inbox).markClaimedPermanentFailure(eq(entry), anyString(), eq(failure));
    }

    private OrderTradeExecutedReconciler reconciler() {
        return new OrderTradeExecutedReconciler(inbox, orderEventSourcingService, 100, 30_000L, 20);
    }

    private TradeExecutedEvent event(String tradeId) {
        return TradeExecutedEvent.builder()
                .tradeId(tradeId)
                .legacyMatchId(1)
                .buyerOrderId(UUID.randomUUID())
                .sellerOrderId(UUID.randomUUID())
                .dealPrice(100)
                .quantity(10)
                .occurredAt(LocalDateTime.now())
                .build();
    }
}
