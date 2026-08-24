package com.eap.eap_order.eventstore;

public class CancellationPrerequisiteNotReadyException extends RuntimeException {

    public CancellationPrerequisiteNotReadyException(String message) {
        super(message);
    }
}
