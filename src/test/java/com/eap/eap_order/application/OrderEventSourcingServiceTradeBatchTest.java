package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
import com.eap.eap_order.eventstore.OrderEventAppender;
import com.eap.eap_order.eventstore.OrderEventAppender.TradeApplicationBatchAppendResult;
import com.eap.eap_order.eventstore.OrderEventAppender.TradeApplicationBatchAppendStatus;
import com.eap.eap_order.eventstore.OrderEventAppender.TradeApplicationBatchNotBatchableReason;
import com.eap.eap_order.eventstore.OrderEventStreamReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderEventSourcingServiceTradeBatchTest {

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
    void missingPrerequisite_shouldDeferWholeBatchWithoutPartialIndividualApply() {
        List<TradeExecutedEvent> events = List.of(event("trade-1"), event("trade-2"));
        when(appender.appendTradeMatchedBatchFromCaughtUpProjectionIfTradeApplicationsAbsent(anyList()))
                .thenReturn(new TradeApplicationBatchAppendResult(
                        TradeApplicationBatchAppendStatus.NOT_BATCHABLE,
                        0,
                        TradeApplicationBatchNotBatchableReason.MISSING_HEAD));

        assertThatThrownBy(() -> service.applyTrades(events))
                .isInstanceOf(TradeProjectionNotReadyException.class)
                .hasMessageContaining("size=2");

        verify(appender).appendTradeMatchedBatchFromCaughtUpProjectionIfTradeApplicationsAbsent(anyList());
        verifyNoMoreInteractions(appender);
        verify(batchMetrics).fallback("missing_head", 2);
    }

    private TradeExecutedEvent event(String tradeId) {
        return TradeExecutedEvent.builder()
                .tradeId(tradeId)
                .legacyMatchId(1)
                .buyerOrderId(UUID.randomUUID())
                .sellerOrderId(UUID.randomUUID())
                .dealPrice(100)
                .quantity(1)
                .occurredAt(LocalDateTime.now())
                .build();
    }
}
