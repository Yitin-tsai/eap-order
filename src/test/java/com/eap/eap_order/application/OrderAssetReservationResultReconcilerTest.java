package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationSucceededEvent;
import com.eap.eap_order.controller.OrderStatusController;
import com.eap.eap_order.eventstore.OrderEventIdentityConflictException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.TransientDataAccessResourceException;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderAssetReservationResultReconcilerTest {

    @Mock
    private OrderAssetReservationResultInbox inbox;
    @Mock
    private OrderAssetReservationResultProcessor processor;
    @Mock
    private OrderStatusController statusController;

    private OrderAssetReservationResultReconciler reconciler;

    @BeforeEach
    void setUp() {
        reconciler = new OrderAssetReservationResultReconciler(
                inbox,
                processor,
                new OrderAssetReservationResultErrorClassifier(),
                statusController,
                10,
                30_000,
                5,
                100,
                10_000,
                base -> base);
    }

    @Test
    void successfulResult_shouldApplyAndNotifyAfterDurableProcessing() {
        OrderAssetReservationResultInbox.InboxEntry entry = entry(1);
        when(inbox.claimRetryable(eq(10), anyString(), eq(30_000L))).thenReturn(List.of(entry));

        reconciler.reconcile();

        verify(processor).process(eq(entry), anyString());
        verify(statusController).updateOrderStatus(
                entry.orderId(), "WALLET_CHECK_PASSED", "餘額檢查通過，已進入撮合佇列");
    }

    @Test
    void transientDatabaseFailure_shouldRecordRetryWithBackoff() {
        OrderAssetReservationResultInbox.InboxEntry entry = entry(2);
        TransientDataAccessResourceException failure =
                new TransientDataAccessResourceException("database restarting");
        when(inbox.claimRetryable(eq(10), anyString(), eq(30_000L))).thenReturn(List.of(entry));
        doThrow(failure).when(processor).process(eq(entry), anyString());

        reconciler.reconcile();

        verify(inbox).reschedule(
                eq(entry), anyString(), eq("TRANSIENT_DATABASE"), eq(failure), eq(200L));
    }

    @Test
    void identityConflict_shouldBecomePermanentWithoutRetry() {
        OrderAssetReservationResultInbox.InboxEntry entry = entry(1);
        OrderEventIdentityConflictException failure =
                new OrderEventIdentityConflictException(UUID.randomUUID());
        when(inbox.claimRetryable(eq(10), anyString(), eq(30_000L))).thenReturn(List.of(entry));
        doThrow(failure).when(processor).process(eq(entry), anyString());

        reconciler.reconcile();

        verify(inbox).markPermanent(
                eq(entry), anyString(), eq("PERMANENT_IDENTITY_CONFLICT"), eq(failure));
    }

    @Test
    void unknownFailureAtRetryBudget_shouldBecomePermanentDebt() {
        OrderAssetReservationResultInbox.InboxEntry entry = entry(5);
        RuntimeException failure = new RuntimeException("unknown failure");
        when(inbox.claimRetryable(eq(10), anyString(), eq(30_000L))).thenReturn(List.of(entry));
        doThrow(failure).when(processor).process(eq(entry), anyString());

        reconciler.reconcile();

        verify(inbox).markPermanent(
                eq(entry), anyString(), eq("RETRY_EXHAUSTED_UNKNOWN_RETRYABLE"), eq(failure));
    }

    private OrderAssetReservationResultInbox.InboxEntry entry(int attemptCount) {
        UUID orderId = UUID.randomUUID();
        OrderAssetReservationSucceededEvent event = OrderAssetReservationSucceededEvent.builder()
                .orderId(orderId)
                .userId(UUID.randomUUID())
                .build();
        return new OrderAssetReservationResultInbox.InboxEntry(
                orderId,
                OrderAssetReservationResultInbox.ResultType.CONFIRMED,
                event,
                null,
                attemptCount);
    }
}
