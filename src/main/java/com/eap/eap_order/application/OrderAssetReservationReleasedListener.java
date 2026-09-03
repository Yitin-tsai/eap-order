package com.eap.eap_order.application;

import com.eap.common.event.OrderAssetReservationReleasedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import static com.eap.common.constants.RabbitMQConstants.ORDER_ASSET_RESERVATION_RELEASED_QUEUE;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderAssetReservationReleasedListener {

    private final OrderAssetReservationReleasedInbox inbox;

    @RabbitListener(
            queues = ORDER_ASSET_RESERVATION_RELEASED_QUEUE,
            concurrency = "${eap.order.listeners.asset-reservation-released.concurrency:4}")
    public void onReleased(OrderAssetReservationReleasedEvent event) {
        OrderAssetReservationReleasedInbox.ReceiveOutcome outcome = inbox.receive(event);
        if (outcome == OrderAssetReservationReleasedInbox.ReceiveOutcome.CONFLICT) {
            log.error("Order release-event identity conflict: cancellationId={}", event.getCancellationId());
        }
    }
}
