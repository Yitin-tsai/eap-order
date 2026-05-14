package com.eap.eap_order.application;

import com.eap.eap_order.configuration.repository.AuditEventRepository;
import com.eap.eap_order.controller.dto.res.OrderEventDto;
import com.eap.eap_order.controller.dto.res.OrderStateDto;
import com.eap.eap_order.domain.entity.AuditEventEntity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderReplayService {

    private final AuditEventRepository auditEventRepository;
    private final ObjectMapper objectMapper;

    private static final Map<String, String> EVENT_TO_STATUS = Map.of(
            "ORDER_SUBMITTED", "SUBMITTED",
            "ORDER_CONFIRMED", "CONFIRMED",
            "ORDER_MATCHED", "MATCHED",
            "ORDER_FAILED", "FAILED",
            "ORDER_CANCELLED", "CANCELLED"
    );

    public OrderStateDto replay(String orderId) {
        List<AuditEventEntity> events = auditEventRepository.findByCorrelationIdOrderByIdAsc(orderId);
        if (events.isEmpty()) {
            return null;
        }

        OrderStateDto state = OrderStateDto.builder()
                .orderId(UUID.fromString(orderId))
                .timeline(new ArrayList<>())
                .build();

        for (AuditEventEntity event : events) {
            applyEvent(state, event);
        }

        return state;
    }

    public List<OrderStateDto> replayByUser(UUID userId) {
        List<AuditEventEntity> allEvents = auditEventRepository.findByUserIdOrderByIdAsc(userId);

        // Group by correlationId (orderId), preserving insertion order
        Map<String, List<AuditEventEntity>> grouped = allEvents.stream()
                .collect(Collectors.groupingBy(
                        AuditEventEntity::getCorrelationId,
                        LinkedHashMap::new,
                        Collectors.toList()
                ));

        return grouped.entrySet().stream()
                .map(entry -> {
                    OrderStateDto state = OrderStateDto.builder()
                            .orderId(parseUuidSafe(entry.getKey()))
                            .timeline(new ArrayList<>())
                            .build();
                    for (AuditEventEntity event : entry.getValue()) {
                        applyEvent(state, event);
                    }
                    return state;
                })
                .toList();
    }

    private void applyEvent(OrderStateDto state, AuditEventEntity event) {
        String status = EVENT_TO_STATUS.getOrDefault(event.getEventType(), event.getEventType());
        state.setStatus(status);
        state.setUpdatedAt(event.getCreatedAt());

        if (event.getUserId() != null) {
            state.setUserId(event.getUserId());
        }

        try {
            JsonNode payload = objectMapper.readTree(event.getPayload());

            switch (event.getEventType()) {
                case "ORDER_SUBMITTED" -> {
                    state.setCreatedAt(event.getCreatedAt());
                    if (payload.has("price")) state.setPrice(payload.get("price").asInt());
                    if (payload.has("amount")) state.setAmount(payload.get("amount").asInt());
                    if (payload.has("orderType")) state.setOrderType(payload.get("orderType").asText());
                }
                case "ORDER_MATCHED" -> {
                    if (payload.has("dealPrice")) state.setDealPrice(payload.get("dealPrice").asInt());
                }
                case "ORDER_FAILED" -> {
                    if (payload.has("reason")) state.setFailReason(payload.get("reason").asText());
                }
                default -> { /* CONFIRMED, CANCELLED — status update only */ }
            }
        } catch (Exception e) {
            log.warn("Failed to parse audit payload for event id={}: {}", event.getId(), e.getMessage());
        }

        state.getTimeline().add(OrderEventDto.builder()
                .eventType(event.getEventType())
                .status(status)
                .timestamp(event.getCreatedAt())
                .build());
    }

    private UUID parseUuidSafe(String value) {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
