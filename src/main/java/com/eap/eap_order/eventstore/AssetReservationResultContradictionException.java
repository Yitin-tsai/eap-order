package com.eap.eap_order.eventstore;

/**
 * Raised when a Wallet result contradicts a stronger Order fact that was already accepted.
 */
public class AssetReservationResultContradictionException extends IllegalStateException {

    public AssetReservationResultContradictionException(String message) {
        super(message);
    }
}
