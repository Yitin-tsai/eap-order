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
import com.eap.eap_order.eventstore.OrderEventAppender.TradeApplicationBatchAppendCommand;
import com.eap.eap_order.eventstore.OrderEventAppender.TradeApplicationBatchAppendResult;
import com.eap.eap_order.eventstore.OrderEventAppender.TradeApplicationBatchAppendStatus;
import com.eap.eap_order.eventstore.OrderEventAppender.TradeExecutionAppendResult;
import com.eap.eap_order.eventstore.OrderEventAppender.TradeExecutionAppendStatus;
import com.eap.eap_order.eventstore.OrderEventStreamReader;
import com.eap.eap_order.eventstore.OrderIntegrationEvent;
import com.eap.eap_order.eventstore.OrderTradeApplication;
import com.eap.eap_order.eventstore.OrderTradeExecutionLink;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.eap.common.constants.RabbitMQConstants.ORDER_EXCHANGE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_SUBMITTED_KEY;
import static com.eap.common.constants.RabbitMQConstants.TRADE_EXCHANGE;
import static com.eap.common.constants.RabbitMQConstants.TRADE_ORDER_APPLIED_KEY;

@Service
public class OrderEventSourcingService {

    private final OrderEventAppender appender;
    private final OrderEventStreamReader streamReader;
    private final OrderTradeBatchMetrics batchMetrics;
    private final boolean fastMatchFromProjectionEnabled;
    private final String tradeIdempotencySource;

    public OrderEventSourcingService(
            OrderEventAppender appender,
            OrderEventStreamReader streamReader,
            OrderTradeBatchMetrics batchMetrics,
            @Value("${eap.order.event-sourcing.fast-match-from-projection.enabled:false}")
            boolean fastMatchFromProjectionEnabled,
            @Value("${eap.order.event-sourcing.trade-idempotency-source:links}")
            String tradeIdempotencySource) {
        this.appender = appender;
        this.streamReader = streamReader;
        this.batchMetrics = batchMetrics;
        this.fastMatchFromProjectionEnabled = fastMatchFromProjectionEnabled;
        this.tradeIdempotencySource = tradeIdempotencySource;
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
        applyPreparedTrade(preparedTrade(source));
    }

    public void applyTrades(List<TradeExecutedEvent> sources) {
        if (sources == null || sources.isEmpty()) {
            return;
        }
        batchMetrics.received(sources.size());
        if (sources.size() == 1) {
            applyTrade(sources.get(0));
            batchMetrics.fallback("singleton_batch", 1);
            return;
        }
        List<PreparedTrade> preparedTrades = new ArrayList<>(sources.size());
        for (TradeExecutedEvent source : sources) {
            preparedTrades.add(preparedTrade(source));
        }
        if (!tradeApplicationIdempotencyEnabled()) {
            applyTradesIndividually(preparedTrades);
            batchMetrics.fallback("non_trade_application_mode", preparedTrades.size());
            return;
        }
        if (hasOverlappingOrders(preparedTrades)) {
            applyTradesIndividually(preparedTrades);
            batchMetrics.fallback("overlapping_order", preparedTrades.size());
            batchMetrics.overlapFallback(preparedTrades.size());
            return;
        }
        TradeApplicationBatchAppendResult result = appender
                .appendTradeMatchedBatchFromCaughtUpProjectionIfTradeApplicationsAbsent(
                        preparedTrades.stream()
                                .map(PreparedTrade::toBatchAppendCommand)
                                .toList());
        if (result.status() == TradeApplicationBatchAppendStatus.APPLIED) {
            batchMetrics.batchApplied(result.appliedCount());
            return;
        }
        applyTradesIndividually(preparedTrades);
        batchMetrics.fallback(result.notBatchableReason().metricName(), preparedTrades.size());
    }

    private void applyTradesIndividually(List<PreparedTrade> preparedTrades) {
        for (PreparedTrade preparedTrade : preparedTrades) {
            applyPreparedTrade(preparedTrade);
        }
    }

    private boolean hasOverlappingOrders(List<PreparedTrade> preparedTrades) {
        Set<UUID> orderIds = new HashSet<>(preparedTrades.size() * 2);
        for (PreparedTrade preparedTrade : preparedTrades) {
            if (!orderIds.add(preparedTrade.source().getBuyerOrderId())
                    || !orderIds.add(preparedTrade.source().getSellerOrderId())) {
                return true;
            }
        }
        return false;
    }

    private PreparedTrade preparedTrade(TradeExecutedEvent source) {
        if (source.getBuyerOrderId() == null || source.getSellerOrderId() == null) {
            throw new IllegalArgumentException("TradeExecutedEvent must contain both buyerOrderId and sellerOrderId");
        }
        if (source.getTradeId() == null || source.getTradeId().isBlank()) {
            throw new IllegalArgumentException("TradeExecutedEvent must contain tradeId");
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
        OrderTradeApplication tradeApplication = tradeApplication(source, occurredAt);
        return new PreparedTrade(
                source,
                buyerFastPathEvent,
                sellerFastPathEvent,
                integrationEvent,
                buyerLink,
                sellerLink,
                tradeApplication);
    }

    private void applyPreparedTrade(PreparedTrade preparedTrade) {
        TradeExecutedEvent source = preparedTrade.source();
        if (eventStoreTradeIdempotencyEnabled()) {
            OrderEventAppendCommand buyerCommand = tradeMatchedCommand(
                    source.getBuyerOrderId(), source.getTradeId(), preparedTrade.buyerFastPathEvent(),
                    preparedTrade.buyerFastPathEvent().matchedAt());
            OrderEventAppendCommand sellerCommand = tradeMatchedCommand(
                    source.getSellerOrderId(), source.getTradeId(), preparedTrade.sellerFastPathEvent(),
                    preparedTrade.sellerFastPathEvent().matchedAt());
            TradeExecutionAppendResult result =
                    appender.appendTradeMatchedFromCaughtUpProjectionWithEventStoreIdempotency(
                            buyerCommand, source.getQuantity(),
                            sellerCommand, source.getQuantity(),
                            preparedTrade.integrationEvent());
            if (result.status() == TradeExecutionAppendStatus.APPLIED
                    || result.status() == TradeExecutionAppendStatus.DUPLICATE) {
                return;
            }
            throw new IllegalStateException("TradeExecutedEvent could not be applied from command state: tradeId="
                    + source.getTradeId());
        }
        if (tradeApplicationIdempotencyEnabled()
                && applyTradeFromCaughtUpProjection(source, preparedTrade.buyerFastPathEvent(),
                preparedTrade.sellerFastPathEvent(), preparedTrade.integrationEvent(),
                preparedTrade.tradeApplication(), source.getQuantity())) {
            return;
        }
        if (fastMatchFromProjectionEnabled
                && applyTradeFromCaughtUpProjection(source, preparedTrade.buyerFastPathEvent(),
                preparedTrade.sellerFastPathEvent(), preparedTrade.integrationEvent(),
                preparedTrade.buyerLink(), preparedTrade.sellerLink(), source.getQuantity())) {
            return;
        }

        OrderAggregate buyerAggregate = streamReader.load(source.getBuyerOrderId());
        OrderAggregate sellerAggregate = streamReader.load(source.getSellerOrderId());
        long buyerExpectedVersion = buyerAggregate.version();
        long sellerExpectedVersion = sellerAggregate.version();
        OrderMatchedV1 buyerEvent = buyerAggregate.match(
                source.getLegacyMatchId(), source.getQuantity(), source.getDealPrice(),
                preparedTrade.buyerFastPathEvent().matchedAt());
        OrderMatchedV1 sellerEvent = sellerAggregate.match(
                source.getLegacyMatchId(), source.getQuantity(), source.getDealPrice(),
                preparedTrade.sellerFastPathEvent().matchedAt());
        TradeExecutionAppendResult fallbackResult = appendTradeFromConsumerIfTradeLinksAbsent(
                source.getBuyerOrderId(),
                buyerExpectedVersion,
                tradeMatchedEventId(source.getBuyerOrderId(), source.getTradeId()),
                buyerEvent,
                buyerAggregate.userId(),
                source.getSellerOrderId(),
                sellerExpectedVersion,
                tradeMatchedEventId(source.getSellerOrderId(), source.getTradeId()),
                sellerEvent,
                sellerAggregate.userId(),
                preparedTrade.buyerFastPathEvent().matchedAt(),
                preparedTrade.integrationEvent(),
                preparedTrade.buyerLink(),
                preparedTrade.sellerLink());
        if (fallbackResult.status() == TradeExecutionAppendStatus.DUPLICATE) {
            return;
        }
    }

    private boolean eventStoreTradeIdempotencyEnabled() {
        return "event-store".equalsIgnoreCase(tradeIdempotencySource);
    }

    private boolean tradeApplicationIdempotencyEnabled() {
        return "trade-application".equalsIgnoreCase(tradeIdempotencySource);
    }

    private OrderEventAppendCommand tradeMatchedCommand(
            UUID orderId,
            String tradeId,
            OrderMatchedV1 event,
            LocalDateTime occurredAt) {
        return new OrderEventAppendCommand(
                orderId,
                0,
                tradeMatchedEventId(orderId, tradeId),
                "OrderMatchedV1",
                event,
                Map.of("correlationId", orderId.toString()),
                1,
                occurredAt,
                null);
    }

    private boolean applyTradeFromCaughtUpProjection(
            TradeExecutedEvent source,
            OrderMatchedV1 buyerEvent,
            OrderMatchedV1 sellerEvent,
            OrderIntegrationEvent integrationEvent,
            OrderTradeApplication tradeApplication,
            int quantity) {
        OrderEventAppendCommand buyerCommand = new OrderEventAppendCommand(
                source.getBuyerOrderId(),
                0,
                tradeMatchedEventId(source.getBuyerOrderId(), source.getTradeId()),
                "OrderMatchedV1",
                buyerEvent,
                Map.of("correlationId", source.getBuyerOrderId().toString()),
                1,
                buyerEvent.matchedAt(),
                null);
        OrderEventAppendCommand sellerCommand = new OrderEventAppendCommand(
                source.getSellerOrderId(),
                0,
                tradeMatchedEventId(source.getSellerOrderId(), source.getTradeId()),
                "OrderMatchedV1",
                sellerEvent,
                Map.of("correlationId", source.getSellerOrderId().toString()),
                1,
                sellerEvent.matchedAt(),
                null);
        TradeExecutionAppendResult result =
                appender.appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsent(
                        buyerCommand, quantity,
                        sellerCommand, quantity,
                        tradeApplication,
                        integrationEvent);
        return result.status() == TradeExecutionAppendStatus.APPLIED
                || result.status() == TradeExecutionAppendStatus.DUPLICATE;
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
                tradeMatchedEventId(source.getBuyerOrderId(), source.getTradeId()),
                "OrderMatchedV1",
                buyerEvent,
                Map.of("correlationId", source.getBuyerOrderId().toString()),
                1,
                buyerEvent.matchedAt(),
                null);
        OrderEventAppendCommand sellerCommand = new OrderEventAppendCommand(
                source.getSellerOrderId(),
                0,
                tradeMatchedEventId(source.getSellerOrderId(), source.getTradeId()),
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

    private OrderTradeApplication tradeApplication(
            TradeExecutedEvent source,
            LocalDateTime appliedAt) {
        return new OrderTradeApplication(
                source.getTradeId(),
                source.getBuyerOrderId(),
                source.getSellerOrderId(),
                source.getDealPrice(),
                source.getQuantity(),
                appliedAt);
    }

    private record PreparedTrade(
            TradeExecutedEvent source,
            OrderMatchedV1 buyerFastPathEvent,
            OrderMatchedV1 sellerFastPathEvent,
            OrderIntegrationEvent integrationEvent,
            OrderTradeExecutionLink buyerLink,
            OrderTradeExecutionLink sellerLink,
            OrderTradeApplication tradeApplication) {

        private TradeApplicationBatchAppendCommand toBatchAppendCommand() {
            return new TradeApplicationBatchAppendCommand(
                    new OrderEventAppendCommand(
                            source.getBuyerOrderId(),
                            0,
                            tradeMatchedEventId(source.getBuyerOrderId(), source.getTradeId()),
                            "OrderMatchedV1",
                            buyerFastPathEvent,
                            Map.of("correlationId", source.getBuyerOrderId().toString()),
                            1,
                            buyerFastPathEvent.matchedAt(),
                            null),
                    source.getQuantity(),
                    new OrderEventAppendCommand(
                            source.getSellerOrderId(),
                            0,
                            tradeMatchedEventId(source.getSellerOrderId(), source.getTradeId()),
                            "OrderMatchedV1",
                            sellerFastPathEvent,
                            Map.of("correlationId", source.getSellerOrderId().toString()),
                            1,
                            sellerFastPathEvent.matchedAt(),
                            null),
                    source.getQuantity(),
                    tradeApplication,
                    integrationEvent);
        }
    }

    public void cancel(UUID orderId, UUID userId) {
        LocalDateTime occurredAt = LocalDateTime.now();
        OrderCancelledV1 event = new OrderCancelledV1(orderId, userId, occurredAt);
        appender.appendCancellationIfCurrentStateAllows(new OrderEventAppendCommand(
                orderId,
                0,
                eventId(orderId, "CANCELLED"),
                "OrderCancelledV1",
                event,
                Map.of("correlationId", orderId.toString(), "userId", userId.toString()),
                1,
                occurredAt,
                null));
    }

    public void assertCancellationAllowed(UUID orderId, UUID userId) {
        appender.assertCancellationAllowed(orderId, userId);
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

    private static UUID eventId(UUID orderId, String discriminator) {
        return UUID.nameUUIDFromBytes(
                (orderId + ":" + discriminator).getBytes(StandardCharsets.UTF_8));
    }

    private static UUID tradeMatchedEventId(UUID orderId, String tradeId) {
        return eventId(orderId, "TRADE_EXECUTED:" + tradeId);
    }

}
