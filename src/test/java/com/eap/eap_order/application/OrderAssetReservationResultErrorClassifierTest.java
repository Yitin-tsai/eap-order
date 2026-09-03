package com.eap.eap_order.application;

import com.eap.eap_order.eventstore.AssetReservationResultContradictionException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderAssetReservationResultErrorClassifierTest {

    private final OrderAssetReservationResultErrorClassifier classifier =
            new OrderAssetReservationResultErrorClassifier();

    @Test
    void contradictionAfterAcceptedTrade_shouldBePermanent() {
        OrderAssetReservationResultErrorClassifier.Classification classification = classifier.classify(
                new AssetReservationResultContradictionException("trade already accepted"));

        assertThat(classification.retryable()).isFalse();
        assertThat(classification.errorType()).isEqualTo("PERMANENT_RESULT_CONTRADICTION");
    }
}
