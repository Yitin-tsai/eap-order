package com.eap.eap_order.application;

import com.eap.eap_order.eventstore.OrderEventIdentityConflictException;
import com.eap.eap_order.eventstore.OrderEventVersionConflictException;
import com.eap.eap_order.eventstore.AssetReservationResultContradictionException;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

@Component
public class OrderAssetReservationResultErrorClassifier {

    public Classification classify(Exception failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof DataAccessException) {
                return new Classification(true, "TRANSIENT_DATABASE");
            }
            if (current instanceof OrderEventIdentityConflictException) {
                return new Classification(false, "PERMANENT_IDENTITY_CONFLICT");
            }
            if (current instanceof OrderEventVersionConflictException) {
                return new Classification(false, "PERMANENT_STATE_CONFLICT");
            }
            if (current instanceof AssetReservationResultContradictionException) {
                return new Classification(false, "PERMANENT_RESULT_CONTRADICTION");
            }
            if (current instanceof IllegalArgumentException) {
                return new Classification(false, "PERMANENT_INVALID_EVENT");
            }
            current = current.getCause();
        }
        return new Classification(true, "UNKNOWN_RETRYABLE");
    }

    public record Classification(boolean retryable, String errorType) {
    }
}
