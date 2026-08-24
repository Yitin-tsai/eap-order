package com.eap.eap_order.application;

import com.eap.common.event.OrderCancellationResultEvent;
import com.eap.eap_order.eventstore.OrderEventAppender;
import com.eap.eap_order.eventstore.OrderEventStreamReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class OrderEventSourcingServiceCancellationTest {

    @Mock
    private OrderEventAppender appender;
    @Mock
    private OrderEventStreamReader streamReader;
    @Mock
    private OrderTradeBatchMetrics batchMetrics;

    private OrderEventSourcingService service;

    @BeforeEach
    void setUp() {
        service = new OrderEventSourcingService(appender, streamReader, batchMetrics);
    }

    @Test
    void alreadyMatchedResult_shouldWaitForNormalTradeApplication() {
        service.applyCancellationResult(result(OrderCancellationResultEvent.ALREADY_MATCHED));

        verifyNoInteractions(appender);
    }

    @Test
    void notOpenWithoutDurableTrade_shouldRemainVisibleAsConsistencyFailure() {
        OrderCancellationResultEvent result = result(OrderCancellationResultEvent.NOT_OPEN);

        assertThatThrownBy(() -> service.applyCancellationResult(result))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("neither an open remainder nor a durable trade");
        verifyNoInteractions(appender);
    }

    private OrderCancellationResultEvent result(String outcome) {
        return OrderCancellationResultEvent.builder()
                .cancellationId(UUID.randomUUID())
                .orderId(UUID.randomUUID())
                .userId(UUID.randomUUID())
                .outcome(outcome)
                .build();
    }
}
