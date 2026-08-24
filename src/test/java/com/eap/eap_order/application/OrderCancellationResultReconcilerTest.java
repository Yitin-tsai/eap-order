package com.eap.eap_order.application;

import com.eap.common.event.OrderCancellationResultEvent;
import com.eap.eap_order.eventstore.CancellationPrerequisiteNotReadyException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderCancellationResultReconcilerTest {

    @Mock
    private OrderCancellationResultInbox inbox;
    @Mock
    private OrderEventSourcingService service;

    private OrderCancellationResultReconciler reconciler;

    @BeforeEach
    void setUp() {
        reconciler = new OrderCancellationResultReconciler(inbox, service, 10, 30_000, 5);
    }

    @Test
    void cancellationAheadOfTradeProjection_shouldRescheduleAsPendingPrerequisite() {
        OrderCancellationResultInbox.InboxEntry entry = entry(1);
        when(inbox.claimRetryable(eq(10), anyString(), eq(30_000L))).thenReturn(List.of(entry));
        CancellationPrerequisiteNotReadyException notReady =
                new CancellationPrerequisiteNotReadyException("trade projection is behind");
        doThrow(notReady).when(service).applyCancellationResult(entry.event());

        reconciler.reconcile();

        verify(inbox).reschedule(eq(entry), anyString(), eq("PENDING_PREREQUISITE"), eq(notReady), eq(100L));
    }

    @Test
    void appliedCancellation_shouldMarkInboxApplied() {
        OrderCancellationResultInbox.InboxEntry entry = entry(2);
        when(inbox.claimRetryable(eq(10), anyString(), eq(30_000L))).thenReturn(List.of(entry));
        when(inbox.markApplied(eq(entry), anyString())).thenReturn(true);

        reconciler.reconcile();

        verify(service).applyCancellationResult(entry.event());
        verify(inbox).markApplied(eq(entry), anyString());
    }

    private OrderCancellationResultInbox.InboxEntry entry(int attempt) {
        return new OrderCancellationResultInbox.InboxEntry(
                OrderCancellationResultEvent.builder()
                        .cancellationId(UUID.randomUUID())
                        .orderId(UUID.randomUUID())
                        .userId(UUID.randomUUID())
                        .outcome(OrderCancellationResultEvent.CANCELLED)
                        .orderType("BUY")
                        .limitPrice(100)
                        .cancelledAmount(6)
                        .decidedAt(LocalDateTime.now())
                        .build(),
                attempt);
    }
}
