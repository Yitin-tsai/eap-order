package com.eap.eap_order.application;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
public class OrderAssetReservationReleasedProcessor {

    private final OrderEventSourcingService orderEventSourcingService;
    private final OrderAssetReservationReleasedInbox inbox;

    @Transactional(transactionManager = "orderConsumerTransactionManager")
    public void process(OrderAssetReservationReleasedInbox.InboxEntry entry, String owner) {
        orderEventSourcingService.completeCancellation(entry.event());
        if (!inbox.markApplied(entry, owner)) {
            throw new IllegalStateException(
                    "Lost asset-release inbox lease before APPLIED: cancellationId="
                            + entry.event().getCancellationId());
        }
    }
}
