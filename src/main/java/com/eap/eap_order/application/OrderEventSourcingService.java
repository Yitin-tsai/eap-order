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
import com.eap.eap_order.eventstore.OrderEventAppender.TradeExecutionAppendResult;
import com.eap.eap_order.eventstore.OrderEventAppender.TradeExecutionAppendStatus;
import com.eap.eap_order.eventstore.OrderEventStreamReader;
import com.eap.eap_order.eventstore.OrderIntegrationEvent;
import com.eap.eap_order.eventstore.OrderTradeExecutionLink;
import org.springframework.beans.factory.annotation.Value;
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
    private final boolean fastMatchFromProjectionEnabled;

    public OrderEventSourcingService(
            OrderEventAppender appender,
            OrderEventStreamReader streamReader,
            @Value("${eap.order.event-sourcing.fast-match-from-projection.enabled:false}")
            boolean fastMatchFromProjectionEnabled) {
        this.appender = appender;
        this.streamReader = streamReader;
        this.fastMatchFromProjectionEnabled = fastMatchFromProjectionEnabled;
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

    public void applyTrade(TradeExecutedEvent source) {
        if (source.getBuyerOrderId() == null || source.getSellerOrderId() == null) {
            throw new IllegalArgumentException("TradeExecutedEvent must contain both buyerOrderId and sellerOrderId");
        }
        LocalDateTime occurredAt = source.getOccurredAt() == null
                ? LocalDateTime.now()
                : source.getOccurredAt();
        OrderMatchedV1 buyerFastPathEvent = new OrderMatchedV1(
                source.getBuyerOrderId(), source.getLegacyMatchId(), source.getQuantity(), source.getDealPrice(), occurredAt);
        OrderMatchedV1 sellerFastPathEvent = new OrderMatchedV1(
                source.getSellerOrderId(), source.getLegacyMatchId(), source.getQuantity(), source.getDealPrice(), occurredAt);
        OrderIntegrationEvent integrationEvent = tradeAppliedIntegration(source, occurredAt);
        OrderTradeExecutionLink buyerLink = tradeExecutionLink(source.getBuyerOrderId(), source, "BUY", occurredAt);
        OrderTradeExecutionLink sellerLink = tradeExecutionLink(source.getSellerOrderId(), source, "SELL", occurredAt);
        if (fastMatchFromProjectionEnabled
                && applyTradeFromCaughtUpProjection(source, buyerFastPathEvent, sellerFastPathEvent,
                integrationEvent, buyerLink, sellerLink, source.getQuantity())) {
            return;
        }

        OrderAggregate buyerAggregate = streamReader.load(source.getBuyerOrderId());
        OrderAggregate sellerAggregate = streamReader.load(source.getSellerOrderId());
        long buyerExpectedVersion = buyerAggregate.version();
        long sellerExpectedVersion = sellerAggregate.version();
        OrderMatchedV1 buyerEvent = buyerAggregate.match(
                source.getLegacyMatchId(), source.getQuantity(), source.getDealPrice(), occurredAt);
        OrderMatchedV1 sellerEvent = sellerAggregate.match(
                source.getLegacyMatchId(), source.getQuantity(), source.getDealPrice(), occurredAt);
        TradeExecutionAppendResult fallbackResult = appendTradeFromConsumerIfTradeLinksAbsent(
                source.getBuyerOrderId(),
                buyerExpectedVersion,
                eventId(source.getBuyerOrderId(), "MATCHED:" + source.getLegacyMatchId()),
                buyerEvent,
                buyerAggregate.userId(),
                source.getSellerOrderId(),
                sellerExpectedVersion,
                eventId(source.getSellerOrderId(), "MATCHED:" + source.getLegacyMatchId()),
                sellerEvent,
                sellerAggregate.userId(),
                occurredAt,
                integrationEvent,
                buyerLink,
                sellerLink);
        if (fallbackResult.status() == TradeExecutionAppendStatus.DUPLICATE) {
            return;
        }
    }

    private boolean applyTradeFromCaughtUpProjection(
            TradeExecutedEvent source,
            OrderMatchedV1 buyerEvent,
            OrderMatchedV1 sellerEvent,
            OrderIntegrationEvent integrationEvent,
            OrderTradeExecutionLink buyerLink,
            OrderTradeExecutionLink sellerLink,
            int quantity) {
        OrderEventAppendCommand buyerCommand = new OrderEventAppendCommand(
                source.getBuyerOrderId(),
                0,
                eventId(source.getBuyerOrderId(), "MATCHED:" + buyerEvent.matchId()),
                "OrderMatchedV1",
                buyerEvent,
                Map.of("correlationId", source.getBuyerOrderId().toString()),
                1,
                buyerEvent.matchedAt(),
                null);
        OrderEventAppendCommand sellerCommand = new OrderEventAppendCommand(
                source.getSellerOrderId(),
                0,
                eventId(source.getSellerOrderId(), "MATCHED:" + sellerEvent.matchId()),
                "OrderMatchedV1",
                sellerEvent,
                Map.of("correlationId", source.getSellerOrderId().toString()),
                1,
                sellerEvent.matchedAt(),
                null);
        TradeExecutionAppendResult result =
                appender.appendTradeMatchedFromCaughtUpProjectionIfTradeLinksAbsent(
                        buyerCommand, quantity, buyerLink,
                        sellerCommand, quantity, sellerLink,
                        integrationEvent);
        return result.status() == TradeExecutionAppendStatus.APPLIED
                || result.status() == TradeExecutionAppendStatus.DUPLICATE;
    }

    private OrderIntegrationEvent tradeAppliedIntegration(
            TradeExecutedEvent source,
            LocalDateTime occurredAt) {
        OrderTradeAppliedEvent integrationEvent = OrderTradeAppliedEvent.builder()
                .tradeId(source.getTradeId())
                .buyerOrderId(source.getBuyerOrderId())
                .sellerOrderId(source.getSellerOrderId())
                .legacyMatchId(source.getLegacyMatchId())
                .dealPrice(source.getDealPrice())
                .quantity(source.getQuantity())
                .buyerAppliedAt(occurredAt)
                .sellerAppliedAt(occurredAt)
                .appliedAt(occurredAt)
                .build();
        return new OrderIntegrationEvent(TRADE_EXCHANGE, TRADE_ORDER_APPLIED_KEY, integrationEvent);
    }

    private OrderTradeExecutionLink tradeExecutionLink(
            UUID orderId,
            TradeExecutedEvent source,
            String side,
            LocalDateTime appliedAt) {
        return new OrderTradeExecutionLink(
                source.getTradeId(),
                orderId,
                side,
                source.getDealPrice(),
                source.getQuantity(),
                appliedAt);
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

    private TradeExecutionAppendResult appendTradeFromConsumerIfTradeLinksAbsent(
            UUID buyerAggregateId,
            long buyerExpectedVersion,
            UUID buyerEventId,
            Object buyerPayload,
            UUID buyerUserId,
            UUID sellerAggregateId,
            long sellerExpectedVersion,
            UUID sellerEventId,
            Object sellerPayload,
            UUID sellerUserId,
            LocalDateTime occurredAt,
            OrderIntegrationEvent integrationEvent,
            OrderTradeExecutionLink buyerLink,
            OrderTradeExecutionLink sellerLink) {
        OrderEventAppendCommand buyerCommand = new OrderEventAppendCommand(
                buyerAggregateId, buyerExpectedVersion, buyerEventId, "OrderMatchedV1", buyerPayload,
                Map.of("correlationId", buyerAggregateId.toString(), "userId", buyerUserId.toString()),
                1, occurredAt, null);
        OrderEventAppendCommand sellerCommand = new OrderEventAppendCommand(
                sellerAggregateId, sellerExpectedVersion, sellerEventId, "OrderMatchedV1", sellerPayload,
                Map.of("correlationId", sellerAggregateId.toString(), "userId", sellerUserId.toString()),
                1, occurredAt, null);
        return appender.appendTradeFromConsumerIfTradeLinksAbsent(
                buyerCommand, buyerLink, sellerCommand, sellerLink, integrationEvent);
    }

    private UUID eventId(UUID orderId, String discriminator) {
        return UUID.nameUUIDFromBytes(
                (orderId + ":" + discriminator).getBytes(StandardCharsets.UTF_8));
    }

}
