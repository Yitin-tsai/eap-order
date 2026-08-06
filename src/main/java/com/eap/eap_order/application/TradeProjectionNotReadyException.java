package com.eap.eap_order.application;

/**
 * The trade is valid, but the Order service has not consumed the prerequisite
 * confirmation event yet. This is an eventual-consistency condition, not a
 * broker delivery failure.
 */
public class TradeProjectionNotReadyException extends RuntimeException {

    public TradeProjectionNotReadyException(String message) {
        super(message);
    }
}
