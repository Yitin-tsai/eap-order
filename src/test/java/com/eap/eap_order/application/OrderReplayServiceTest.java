package com.eap.eap_order.application;

import com.eap.eap_order.configuration.repository.AuditEventRepository;
import com.eap.eap_order.controller.dto.res.OrderStateDto;
import com.eap.eap_order.domain.entity.AuditEventEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderReplayServiceTest {

    @Mock
    private AuditEventRepository auditEventRepository;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private OrderReplayService orderReplayService;

    private static final String ORDER_ID = "550e8400-e29b-41d4-a716-446655440000";
    private static final UUID USER_ID = UUID.fromString("660e8400-e29b-41d4-a716-446655440001");

    @Test
    @DisplayName("Replay full lifecycle: SUBMITTED → CONFIRMED → MATCHED")
    void replay_fullLifecycle_matched() {
        List<AuditEventEntity> events = List.of(
                buildEvent(1L, "ORDER_SUBMITTED", ORDER_ID, USER_ID,
                        "{\"marketId\":\"ENERGY-SPOT\",\"marketSequence\":10,\"price\":100,\"amount\":5,\"orderType\":\"BUY\"}",
                        LocalDateTime.of(2026, 1, 1, 10, 0)),
                buildEvent(2L, "ORDER_CONFIRMED", ORDER_ID, USER_ID,
                        "{\"orderId\":\"" + ORDER_ID + "\"}",
                        LocalDateTime.of(2026, 1, 1, 10, 1)),
                buildEvent(3L, "ORDER_MATCHED", ORDER_ID, USER_ID,
                        "{\"dealPrice\":98,\"amount\":5}",
                        LocalDateTime.of(2026, 1, 1, 10, 2))
        );
        when(auditEventRepository.findByCorrelationIdOrderByIdAsc(ORDER_ID)).thenReturn(events);

        OrderStateDto state = orderReplayService.replay(ORDER_ID);

        assertThat(state).isNotNull();
        assertThat(state.getStatus()).isEqualTo("MATCHED");
        assertThat(state.getMarketId()).isEqualTo("ENERGY-SPOT");
        assertThat(state.getMarketSequence()).isEqualTo(10L);
        assertThat(state.getPrice()).isEqualTo(100);
        assertThat(state.getAmount()).isEqualTo(5);
        assertThat(state.getOrderType()).isEqualTo("BUY");
        assertThat(state.getDealPrice()).isEqualTo(98);
        assertThat(state.getTimeline()).hasSize(3);
        assertThat(state.getTimeline().get(0).getEventType()).isEqualTo("ORDER_SUBMITTED");
        assertThat(state.getTimeline().get(2).getEventType()).isEqualTo("ORDER_MATCHED");
    }

    @Test
    @DisplayName("Replay failed order: SUBMITTED → FAILED")
    void replay_failedOrder() {
        List<AuditEventEntity> events = List.of(
                buildEvent(1L, "ORDER_SUBMITTED", ORDER_ID, USER_ID,
                        "{\"price\":100,\"amount\":5,\"orderType\":\"BUY\"}",
                        LocalDateTime.of(2026, 1, 1, 10, 0)),
                buildEvent(2L, "ORDER_FAILED", ORDER_ID, USER_ID,
                        "{\"reason\":\"INSUFFICIENT_BALANCE\"}",
                        LocalDateTime.of(2026, 1, 1, 10, 1))
        );
        when(auditEventRepository.findByCorrelationIdOrderByIdAsc(ORDER_ID)).thenReturn(events);

        OrderStateDto state = orderReplayService.replay(ORDER_ID);

        assertThat(state.getStatus()).isEqualTo("FAILED");
        assertThat(state.getFailReason()).isEqualTo("INSUFFICIENT_BALANCE");
        assertThat(state.getTimeline()).hasSize(2);
    }

    @Test
    @DisplayName("Replay cancelled order: SUBMITTED → CONFIRMED → CANCELLED")
    void replay_cancelledOrder() {
        List<AuditEventEntity> events = List.of(
                buildEvent(1L, "ORDER_SUBMITTED", ORDER_ID, USER_ID,
                        "{\"price\":100,\"amount\":5,\"orderType\":\"SELL\"}",
                        LocalDateTime.of(2026, 1, 1, 10, 0)),
                buildEvent(2L, "ORDER_CONFIRMED", ORDER_ID, USER_ID,
                        "{}",
                        LocalDateTime.of(2026, 1, 1, 10, 1)),
                buildEvent(3L, "ORDER_CANCELLED", ORDER_ID, USER_ID,
                        "{\"orderId\":\"" + ORDER_ID + "\"}",
                        LocalDateTime.of(2026, 1, 1, 10, 5))
        );
        when(auditEventRepository.findByCorrelationIdOrderByIdAsc(ORDER_ID)).thenReturn(events);

        OrderStateDto state = orderReplayService.replay(ORDER_ID);

        assertThat(state.getStatus()).isEqualTo("CANCELLED");
        assertThat(state.getOrderType()).isEqualTo("SELL");
        assertThat(state.getTimeline()).hasSize(3);
    }

    @Test
    @DisplayName("Replay non-existent order returns null")
    void replay_notFound_returnsNull() {
        when(auditEventRepository.findByCorrelationIdOrderByIdAsc("non-existent")).thenReturn(List.of());

        OrderStateDto state = orderReplayService.replay("non-existent");

        assertThat(state).isNull();
    }

    @Test
    @DisplayName("replayByUser groups events by correlationId and replays each")
    void replayByUser_groupsAndReplays() {
        String orderId1 = "aaa-111";
        String orderId2 = "bbb-222";
        List<AuditEventEntity> events = List.of(
                buildEvent(1L, "ORDER_SUBMITTED", orderId1, USER_ID,
                        "{\"price\":100,\"amount\":5,\"orderType\":\"BUY\"}",
                        LocalDateTime.of(2026, 1, 1, 10, 0)),
                buildEvent(2L, "ORDER_SUBMITTED", orderId2, USER_ID,
                        "{\"price\":200,\"amount\":3,\"orderType\":\"SELL\"}",
                        LocalDateTime.of(2026, 1, 1, 10, 1)),
                buildEvent(3L, "ORDER_CONFIRMED", orderId1, USER_ID,
                        "{}",
                        LocalDateTime.of(2026, 1, 1, 10, 2)),
                buildEvent(4L, "ORDER_FAILED", orderId2, USER_ID,
                        "{\"reason\":\"INSUFFICIENT_BALANCE\"}",
                        LocalDateTime.of(2026, 1, 1, 10, 3))
        );
        when(auditEventRepository.findByUserIdOrderByIdAsc(USER_ID)).thenReturn(events);

        List<OrderStateDto> orders = orderReplayService.replayByUser(USER_ID);

        assertThat(orders).hasSize(2);
        assertThat(orders.get(0).getStatus()).isEqualTo("CONFIRMED");
        assertThat(orders.get(0).getPrice()).isEqualTo(100);
        assertThat(orders.get(1).getStatus()).isEqualTo("FAILED");
        assertThat(orders.get(1).getPrice()).isEqualTo(200);
        assertThat(orders.get(1).getFailReason()).isEqualTo("INSUFFICIENT_BALANCE");
    }

    @Test
    @DisplayName("Replay only SUBMITTED (in-progress order)")
    void replay_submittedOnly() {
        List<AuditEventEntity> events = List.of(
                buildEvent(1L, "ORDER_SUBMITTED", ORDER_ID, USER_ID,
                        "{\"price\":50,\"amount\":10,\"orderType\":\"BUY\"}",
                        LocalDateTime.of(2026, 1, 1, 10, 0))
        );
        when(auditEventRepository.findByCorrelationIdOrderByIdAsc(ORDER_ID)).thenReturn(events);

        OrderStateDto state = orderReplayService.replay(ORDER_ID);

        assertThat(state.getStatus()).isEqualTo("SUBMITTED");
        assertThat(state.getPrice()).isEqualTo(50);
        assertThat(state.getAmount()).isEqualTo(10);
        assertThat(state.getDealPrice()).isNull();
        assertThat(state.getFailReason()).isNull();
        assertThat(state.getTimeline()).hasSize(1);
    }

    private AuditEventEntity buildEvent(Long id, String eventType, String correlationId,
                                         UUID userId, String payload, LocalDateTime createdAt) {
        AuditEventEntity entity = new AuditEventEntity();
        entity.setId(id);
        entity.setEventType(eventType);
        entity.setCorrelationId(correlationId);
        entity.setUserId(userId);
        entity.setPayload(payload);
        entity.setCreatedAt(createdAt);
        return entity;
    }
}
