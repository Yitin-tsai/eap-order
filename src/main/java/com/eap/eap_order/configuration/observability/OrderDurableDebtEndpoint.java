package com.eap.eap_order.configuration.observability;

import com.eap.common.observability.DurableDebtSnapshot;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

@Component
@Endpoint(id = "durableDebt")
@RequiredArgsConstructor
public class OrderDurableDebtEndpoint {

    private final OrderDurableDebtSnapshotProvider provider;

    @ReadOperation
    public DurableDebtSnapshot snapshot() {
        return provider.snapshot();
    }
}
