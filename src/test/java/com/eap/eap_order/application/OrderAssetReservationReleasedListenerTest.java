package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationReleasedEvent;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderAssetReservationReleasedListenerTest {

    @Test
    void listener_shouldReturnOnlyAfterDurableIntake() {
        OrderAssetReservationReleasedInbox inbox = mock(OrderAssetReservationReleasedInbox.class);
        OrderAssetReservationReleasedListener listener = new OrderAssetReservationReleasedListener(inbox);
        OrderAssetReservationReleasedEvent event = event();
        when(inbox.receive(event)).thenReturn(OrderAssetReservationReleasedInbox.ReceiveOutcome.ACCEPTED);

        listener.onReleased(event);

        verify(inbox).receive(event);
    }

    @Test
    void intakeFailure_shouldEscapeSoBrokerDoesNotAck() {
        OrderAssetReservationReleasedInbox inbox = mock(OrderAssetReservationReleasedInbox.class);
        OrderAssetReservationReleasedListener listener = new OrderAssetReservationReleasedListener(inbox);
        OrderAssetReservationReleasedEvent event = event();
        RuntimeException failure = new RuntimeException("database unavailable");
        doThrow(failure).when(inbox).receive(event);

        assertThatThrownBy(() -> listener.onReleased(event)).isSameAs(failure);
    }

    private OrderAssetReservationReleasedEvent event() {
        return OrderAssetReservationReleasedEvent.builder()
                .eventId(UUID.randomUUID())
                .cancellationId(UUID.randomUUID())
                .orderId(UUID.randomUUID())
                .userId(UUID.randomUUID())
                .releasedQuantity(5)
                .build();
    }
}
