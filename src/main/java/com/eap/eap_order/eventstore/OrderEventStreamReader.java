package com.eap.eap_order.eventstore;

import com.eap.eap_order.domain.ordersourcing.OrderAggregate;
import com.eap.eap_order.domain.ordersourcing.OrderAssetReservationConfirmedV1;
import com.eap.eap_order.domain.ordersourcing.OrderAssetReservationFailedV1;
import com.eap.eap_order.domain.ordersourcing.OrderCancelledV1;
import com.eap.eap_order.domain.ordersourcing.OrderMatchedV1;
import com.eap.eap_order.domain.ordersourcing.OrderSubmissionRequestedV1;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class OrderEventStreamReader {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public OrderEventStreamReader(@Qualifier("jdbcTemplate") JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public OrderAggregate load(UUID orderId) {
        List<StoredEvent> events = jdbc.query("""
                SELECT aggregate_version, event_type, payload_canonical
                FROM order_service.order_event_store
                WHERE aggregate_id = ?
                ORDER BY aggregate_version
                """, (rs, rowNum) -> new StoredEvent(
                rs.getLong("aggregate_version"),
                rs.getString("event_type"),
                rs.getString("payload_canonical")), orderId);
        if (events.isEmpty()) {
            throw new IllegalArgumentException("Order event stream not found: " + orderId);
        }
        OrderAggregate aggregate = new OrderAggregate();
        for (StoredEvent event : events) {
            aggregate.apply(deserialize(event));
            if (aggregate.version() != event.version()) {
                throw new IllegalStateException("Order event stream version gap: orderId=" + orderId);
            }
        }
        return aggregate;
    }

    private Object deserialize(StoredEvent event) {
        Class<?> type = switch (event.eventType()) {
            case "OrderSubmissionRequestedV1" -> OrderSubmissionRequestedV1.class;
            case "OrderAssetReservationConfirmedV1" -> OrderAssetReservationConfirmedV1.class;
            case "OrderAssetReservationFailedV1" -> OrderAssetReservationFailedV1.class;
            case "OrderMatchedV1" -> OrderMatchedV1.class;
            case "OrderCancelledV1" -> OrderCancelledV1.class;
            default -> throw new IllegalArgumentException("Unsupported Order event type: " + event.eventType());
        };
        try {
            return objectMapper.readValue(event.payload(), type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialize Order event: " + event.eventType(), e);
        }
    }

    private record StoredEvent(long version, String eventType, String payload) {
    }
}
