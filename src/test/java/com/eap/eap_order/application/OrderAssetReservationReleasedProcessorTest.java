package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationReleasedEvent;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderAssetReservationReleasedProcessorTest {

    @Test
    void domainCompletionAndInboxApplied_shouldShareTransactionBoundary() {
        OrderEventSourcingService service = mock(OrderEventSourcingService.class);
        OrderAssetReservationReleasedInbox inbox = mock(OrderAssetReservationReleasedInbox.class);
        OrderAssetReservationReleasedProcessor processor =
                new OrderAssetReservationReleasedProcessor(service, inbox);
        OrderAssetReservationReleasedInbox.InboxEntry entry = entry();
        when(inbox.markApplied(entry, "worker")).thenReturn(true);

        processor.process(entry, "worker");

        verify(service).completeCancellation(entry.event());
        verify(inbox).markApplied(entry, "worker");
    }

    @Test
    void lostLease_shouldFailAndLetOuterTransactionRollBackDomainAppend() {
        OrderEventSourcingService service = mock(OrderEventSourcingService.class);
        OrderAssetReservationReleasedInbox inbox = mock(OrderAssetReservationReleasedInbox.class);
        OrderAssetReservationReleasedProcessor processor =
                new OrderAssetReservationReleasedProcessor(service, inbox);
        OrderAssetReservationReleasedInbox.InboxEntry entry = entry();
        when(inbox.markApplied(entry, "worker")).thenReturn(false);

        assertThatThrownBy(() -> processor.process(entry, "worker"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Lost asset-release inbox lease");
    }

    private OrderAssetReservationReleasedInbox.InboxEntry entry() {
        return new OrderAssetReservationReleasedInbox.InboxEntry(
                OrderAssetReservationReleasedEvent.builder()
                        .eventId(UUID.randomUUID())
                        .cancellationId(UUID.randomUUID())
                        .orderId(UUID.randomUUID())
                        .userId(UUID.randomUUID())
                        .releasedQuantity(5)
                        .build(),
                1);
    }
}
