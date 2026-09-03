package com.eap.eap_order.domain.ordersourcing;

/**
 * Tracks the Wallet-owned reservation fact independently from the Order execution lifecycle.
 */
public enum OrderAssetReservationStatus {
    NOT_REQUESTED,
    PENDING,
    SUCCEEDED,
    REJECTED,
    RELEASED
}
