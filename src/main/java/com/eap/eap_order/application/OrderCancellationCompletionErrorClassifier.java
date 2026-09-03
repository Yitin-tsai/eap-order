package com.eap.eap_order.application;

import com.eap.eap_order.eventstore.CancellationPrerequisiteNotReadyException;
import com.eap.eap_order.eventstore.OrderEventIdentityConflictException;
import com.eap.eap_order.eventstore.OrderEventVersionConflictException;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

@Component
public class OrderCancellationCompletionErrorClassifier {

    public Classification classify(Exception failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof CancellationPrerequisiteNotReadyException) {
                return new Classification(true, true, "PENDING_CANCELLATION_DECISION");
            }
            if (current instanceof OrderEventIdentityConflictException) {
                return new Classification(false, false, "PERMANENT_IDENTITY_CONFLICT");
            }
            if (current instanceof OrderEventVersionConflictException) {
                return new Classification(true, false, "TRANSIENT_VERSION_CONFLICT");
            }
            if (current instanceof DataAccessException) {
                return new Classification(true, false, "TRANSIENT_DATABASE");
            }
            if (current instanceof IllegalArgumentException || current instanceof IllegalStateException) {
                return new Classification(false, false, "PERMANENT_STATE_OR_PAYLOAD");
            }
            current = current.getCause();
        }
        return new Classification(true, false, "UNKNOWN_RETRYABLE");
    }

    public record Classification(boolean retryable, boolean prerequisite, String errorType) {
    }
}
