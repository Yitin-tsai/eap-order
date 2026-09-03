package com.eap.eap_order.application;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
public class OrderAssetReservationResultProcessor {

    private final OrderEventSourcingService orderEventSourcingService;
    private final OrderAssetReservationResultInbox inbox;

    @Transactional(transactionManager = "orderConsumerTransactionManager")
    public void process(OrderAssetReservationResultInbox.InboxEntry entry, String owner) {
        switch (entry.resultType()) {
            case CONFIRMED -> orderEventSourcingService.confirm(entry.confirmedEvent());
            case FAILED -> orderEventSourcingService.fail(entry.failedEvent());
        }
        if (!inbox.markApplied(entry, owner)) {
            throw new IllegalStateException(
                    "Lost asset-reservation-result inbox lease before APPLIED: orderId=" + entry.orderId());
        }
    }
}
