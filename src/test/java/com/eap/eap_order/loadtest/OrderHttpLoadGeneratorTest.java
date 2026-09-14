package com.eap.eap_order.loadtest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderHttpLoadGeneratorTest {

    @Test
    void trafficOnlyMode_shouldRejectGeneratorOwnedReset() {
        assertThatThrownBy(() -> OrderHttpLoadGenerator.requireTrafficOnly(true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("traffic-only");
    }

    @Test
    void trafficOnlyMode_shouldAllowPreparedRuntime() {
        assertThatCode(() -> OrderHttpLoadGenerator.requireTrafficOnly(false))
                .doesNotThrowAnyException();
    }
}
