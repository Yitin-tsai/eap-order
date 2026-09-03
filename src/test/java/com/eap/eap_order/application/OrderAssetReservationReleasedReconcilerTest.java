package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationReleasedEvent;
import com.eap.eap_order.eventstore.CancellationPrerequisiteNotReadyException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderAssetReservationReleasedReconcilerTest {

    @Mock OrderAssetReservationReleasedInbox inbox;
    @Mock OrderAssetReservationReleasedProcessor processor;
    @Mock OrderCancellationCompletionErrorClassifier classifier;

    private OrderAssetReservationReleasedReconciler reconciler;

    @BeforeEach
    void setUp() {
        reconciler = new OrderAssetReservationReleasedReconciler(
                inbox, processor, classifier, 10, 30_000, 5);
    }

    @Test
    void releaseBeforeMatchEngineDecision_shouldWaitAsPrerequisite() {
        OrderAssetReservationReleasedInbox.InboxEntry entry = entry(1);
        CancellationPrerequisiteNotReadyException failure =
                new CancellationPrerequisiteNotReadyException("decision has not arrived");
        when(inbox.claimRetryable(eq(10), anyString(), eq(30_000L))).thenReturn(List.of(entry));
        doThrow(failure).when(processor).process(eq(entry), anyString());
        when(classifier.classify(failure)).thenReturn(
                new OrderCancellationCompletionErrorClassifier.Classification(
                        true, true, "PENDING_CANCELLATION_DECISION"));

        reconciler.reconcile();

        verify(inbox).reschedule(eq(entry), anyString(), eq("PENDING_PREREQUISITE"),
                eq("PENDING_CANCELLATION_DECISION"), eq(failure), anyLong());
    }

    @Test
    void unknownFailureAtBudget_shouldBecomePermanent() {
        OrderAssetReservationReleasedInbox.InboxEntry entry = entry(5);
        RuntimeException failure = new RuntimeException("unknown");
        when(inbox.claimRetryable(eq(10), anyString(), eq(30_000L))).thenReturn(List.of(entry));
        doThrow(failure).when(processor).process(eq(entry), anyString());
        when(classifier.classify(failure)).thenReturn(
                new OrderCancellationCompletionErrorClassifier.Classification(
                        true, false, "UNKNOWN_RETRYABLE"));

        reconciler.reconcile();

        verify(inbox).markPermanent(eq(entry), anyString(), eq("UNKNOWN_RETRYABLE"), eq(failure));
    }

    private OrderAssetReservationReleasedInbox.InboxEntry entry(int attempt) {
        return new OrderAssetReservationReleasedInbox.InboxEntry(
                OrderAssetReservationReleasedEvent.builder()
                        .eventId(UUID.randomUUID())
                        .cancellationId(UUID.randomUUID())
                        .orderId(UUID.randomUUID())
                        .userId(UUID.randomUUID())
                        .releasedQuantity(5)
                        .build(),
                attempt);
    }
}
