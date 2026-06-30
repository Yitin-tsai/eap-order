package com.eap.eap_order.application;

import com.eap.common.event.OrderConfirmedEvent;
import com.eap.common.event.OrderFailedEvent;
import com.eap.common.event.OrderMatchedEvent;
import com.eap.common.event.OrderSubmittedEvent;
import com.eap.common.event.OrderTradeAppliedEvent;
import com.eap.common.event.TradeExecutedEvent;
import com.eap.eap_order.domain.ordersourcing.OrderAggregate;
import com.eap.eap_order.domain.ordersourcing.OrderAssetReservationConfirmedV1;
import com.eap.eap_order.domain.ordersourcing.OrderAssetReservationFailedV1;
import com.eap.eap_order.domain.ordersourcing.OrderCancelledV1;
import com.eap.eap_order.domain.ordersourcing.OrderMatchedV1;
import com.eap.eap_order.domain.ordersourcing.OrderSubmissionRequestedV1;
import com.eap.eap_order.eventstore.OrderEventAppendCommand;
import com.eap.eap_order.eventstore.OrderEventAppender;
import com.eap.eap_order.eventstore.OrderEventStreamReader;
import com.eap.eap_order.eventstore.OrderIntegrationEvent;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

import static com.eap.common.constants.RabbitMQConstants.ORDER_EXCHANGE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_SUBMITTED_KEY;
import static com.eap.common.constants.RabbitMQConstants.TRADE_EXCHANGE;
import static com.eap.common.constants.RabbitMQConstants.TRADE_ORDER_APPLIED_KEY;

@Service
public class OrderEventSourcingService {

    private final OrderEventAppender appender;
    private final OrderEventStreamReader streamReader;

    public OrderEventSourcingService(OrderEventAppender appender, OrderEventStreamReader streamReader) {
        this.appender = appender;
        this.streamReader = streamReader;
    }

    public void request(OrderSubmittedEvent integrationEvent) {
        OrderAggregate aggregate = new OrderAggregate();
        LocalDateTime occurredAt = integrationEvent.getCreatedAt() == null
                ? LocalDateTime.now()
                : integrationEvent.getCreatedAt();
        OrderSubmissionRequestedV1 event = aggregate.request(
                integrationEvent.getOrderId(), integrationEvent.getUserId(),
                integrationEvent.getMarketId(), integrationEvent.getMarketSequence(),
                integrationEvent.getOrderType(), integrationEvent.getPrice(),
                integrationEvent.getAmount(), occurredAt);
        append(aggregate.orderId(), 0, eventId(aggregate.orderId(), "REQUESTED"),
                "OrderSubmissionRequestedV1", event, integrationEvent.getUserId(),
                occurredAt, new OrderIntegrationEvent(ORDER_EXCHANGE, ORDER_SUBMITTED_KEY, integrationEvent));
    }

    public void confirm(OrderConfirmedEvent source) {
        LocalDateTime occurredAt = source.getCreatedAt() == null
                ? LocalDateTime.now()
                : source.getCreatedAt();
        OrderAssetReservationConfirmedV1 event = new OrderAssetReservationConfirmedV1(
                source.getOrderId(), source.getUserId(), occurredAt);
        appendFromConsumer(source.getOrderId(), 1, eventId(source.getOrderId(), "ASSET_RESERVATION_CONFIRMED"),
                "OrderAssetReservationConfirmedV1", event, source.getUserId(), occurredAt, null);
    }

    public void fail(OrderFailedEvent source) {
        LocalDateTime occurredAt = source.getFailedAt() == null
                ? LocalDateTime.now()
                : source.getFailedAt();
        OrderAssetReservationFailedV1 event = new OrderAssetReservationFailedV1(
                source.getOrderId(), source.getUserId(), source.getReason(), source.getFailureType(), occurredAt);
        appendFromConsumer(source.getOrderId(), 1, eventId(source.getOrderId(), "ASSET_RESERVATION_FAILED"),
                "OrderAssetReservationFailedV1", event, source.getUserId(), occurredAt, null);
    }

    public void match(UUID orderId, OrderMatchedEvent source) {
        OrderAggregate aggregate = streamReader.load(orderId);
        long expectedVersion = aggregate.version();
        OrderMatchedV1 event = aggregate.match(
                source.getMatchId(), source.getAmount(), source.getDealPrice(), source.getMatchedAt());
        LocalDateTime occurredAt = source.getMatchedAt() == null
                ? LocalDateTime.now()
                : source.getMatchedAt();
        appendFromConsumer(orderId, expectedVersion, eventId(orderId, "MATCHED:" + source.getMatchId()),
                "OrderMatchedV1", event, aggregate.userId(), occurredAt, null);
    }

    public void match(UUID orderId, TradeExecutedEvent source, String side) {
        OrderAggregate aggregate = streamReader.load(orderId);
        long expectedVersion = aggregate.version();
        LocalDateTime occurredAt = source.getOccurredAt() == null
                ? LocalDateTime.now()
                : source.getOccurredAt();
        OrderMatchedV1 event = aggregate.match(
                source.getLegacyMatchId(), source.getQuantity(), source.getDealPrice(), occurredAt);
        OrderTradeAppliedEvent integrationEvent = OrderTradeAppliedEvent.builder()
                .tradeId(source.getTradeId())
                .orderId(orderId)
                .side(side)
                .legacyMatchId(source.getLegacyMatchId())
                .dealPrice(source.getDealPrice())
                .quantity(source.getQuantity())
                .appliedAt(occurredAt)
                .build();
        appendFromConsumer(orderId, expectedVersion, eventId(orderId, "MATCHED:" + source.getLegacyMatchId()),
                "OrderMatchedV1", event, aggregate.userId(), occurredAt,
                new OrderIntegrationEvent(TRADE_EXCHANGE, TRADE_ORDER_APPLIED_KEY, integrationEvent));
    }

    public void cancel(UUID orderId, UUID userId) {
        OrderAggregate aggregate = streamReader.load(orderId);
        long expectedVersion = aggregate.version();
        LocalDateTime occurredAt = LocalDateTime.now();
        OrderCancelledV1 event = aggregate.cancel(userId, occurredAt);
        append(orderId, expectedVersion, eventId(orderId, "CANCELLED"),
                "OrderCancelledV1", event, userId, occurredAt, null);
    }

    private void append(
            UUID aggregateId,
            long expectedVersion,
            UUID eventId,
            String eventType,
            Object payload,
            UUID userId,
            LocalDateTime occurredAt,
            OrderIntegrationEvent integrationEvent) {
        appender.append(new OrderEventAppendCommand(
                aggregateId, expectedVersion, eventId, eventType, payload,
                Map.of("correlationId", aggregateId.toString(), "userId", userId.toString()),
                1, occurredAt, integrationEvent));
    }

    private void appendFromConsumer(
            UUID aggregateId,
            long expectedVersion,
            UUID eventId,
            String eventType,
            Object payload,
            UUID userId,
            LocalDateTime occurredAt,
            OrderIntegrationEvent integrationEvent) {
        appender.appendFromConsumer(new OrderEventAppendCommand(
                aggregateId, expectedVersion, eventId, eventType, payload,
                Map.of("correlationId", aggregateId.toString(), "userId", userId.toString()),
                1, occurredAt, integrationEvent));
    }

    private UUID eventId(UUID orderId, String discriminator) {
        return UUID.nameUUIDFromBytes(
                (orderId + ":" + discriminator).getBytes(StandardCharsets.UTF_8));
    }
}
