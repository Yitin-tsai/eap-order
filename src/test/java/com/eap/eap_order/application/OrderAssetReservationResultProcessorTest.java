package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationSucceededEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderAssetReservationResultProcessorTest {

    @Mock
    private OrderEventSourcingService service;
    @Mock
    private OrderAssetReservationResultInbox inbox;

    @Test
    void process_shouldApplyDomainEventBeforeMarkingInboxApplied() {
        OrderAssetReservationResultInbox.InboxEntry entry = confirmedEntry();
        when(inbox.markApplied(entry, "worker-1")).thenReturn(true);
        OrderAssetReservationResultProcessor processor =
                new OrderAssetReservationResultProcessor(service, inbox);

        processor.process(entry, "worker-1");

        InOrder ordered = inOrder(service, inbox);
        ordered.verify(service).confirm(entry.confirmedEvent());
        ordered.verify(inbox).markApplied(entry, "worker-1");
    }

    @Test
    void process_whenLeaseIsLost_shouldFailSoOuterTransactionRollsBack() {
        OrderAssetReservationResultInbox.InboxEntry entry = confirmedEntry();
        when(inbox.markApplied(entry, "worker-1")).thenReturn(false);
        OrderAssetReservationResultProcessor processor =
                new OrderAssetReservationResultProcessor(service, inbox);

        assertThatThrownBy(() -> processor.process(entry, "worker-1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Lost asset-reservation-result inbox lease");

        verify(service).confirm(entry.confirmedEvent());
    }

    private OrderAssetReservationResultInbox.InboxEntry confirmedEntry() {
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
                1);
    }
}
