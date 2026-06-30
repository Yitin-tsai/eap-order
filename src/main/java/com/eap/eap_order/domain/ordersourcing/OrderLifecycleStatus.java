package com.eap.eap_order.domain.ordersourcing;

public enum OrderLifecycleStatus {
    NOT_CREATED,
    PENDING_ASSET_CHECK,
    OPEN,
    REJECTED,
    PARTIALLY_MATCHED,
    MATCHED,
    CANCELLED
}
