package com.eap.eap_order.application;

import com.eap.common.event.OrderConfirmedEvent;
import com.eap.common.event.OrderFailedEvent;
import com.eap.common.event.OrderSubmittedEvent;
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

@Service
public class OrderEventSourcingService {

    private final OrderEventAppender appender;
    private final OrderEventStreamReader streamReader;
    private final OrderTradeBatchMetrics batchMetrics;

    public OrderEventSourcingService(
            OrderEventAppender appender,
            OrderEventStreamReader streamReader,
            OrderTradeBatchMetrics batchMetrics) {
        this.appender = appender;
        this.streamReader = streamReader;
        this.batchMetrics = batchMetrics;
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
        appender.appendFromConsumer(confirmationCommand(source));
    }

    public void confirmAll(List<OrderConfirmedEvent> sources) {
        if (sources == null || sources.isEmpty()) {
            return;
        }
        if (sources.size() == 1) {
            confirm(sources.get(0));
            return;
        }
        appender.appendFromConsumerBatch(sources.stream()
                .map(this::confirmationCommand)
                .toList());
    }

    private OrderEventAppendCommand confirmationCommand(OrderConfirmedEvent source) {
        LocalDateTime occurredAt = source.getCreatedAt() == null
                ? LocalDateTime.now()
                : source.getCreatedAt();
        OrderAssetReservationConfirmedV1 event = new OrderAssetReservationConfirmedV1(
                source.getOrderId(), source.getUserId(), occurredAt);
        return new OrderEventAppendCommand(
                source.getOrderId(),
                1,
                eventId(source.getOrderId(), "ASSET_RESERVATION_CONFIRMED"),
                "OrderAssetReservationConfirmedV1",
                event,
                Map.of("correlationId", source.getOrderId().toString(), "userId", source.getUserId().toString()),
                1,
                occurredAt,
                null);
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
        OrderTradeApplication tradeApplication = tradeApplication(source, occurredAt);
        return new PreparedTrade(
                source,
                buyerFastPathEvent,
                sellerFastPathEvent,
                tradeApplication);
    }

    private void applyPreparedTrade(PreparedTrade preparedTrade) {
        TradeExecutedEvent source = preparedTrade.source();
        if (applyTradeFromCaughtUpProjection(source, preparedTrade.buyerFastPathEvent(),
                preparedTrade.sellerFastPathEvent(), preparedTrade.tradeApplication(),
                source.getQuantity())) {
            return;
        }
        throw new IllegalStateException("TradeExecutedEvent could not be applied from command state: tradeId="
                + source.getTradeId());
    }

    private boolean applyTradeFromCaughtUpProjection(
            TradeExecutedEvent source,
            OrderMatchedV1 buyerEvent,
            OrderMatchedV1 sellerEvent,
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
                        tradeApplication);
        return result.status() == TradeExecutionAppendStatus.APPLIED
                || result.status() == TradeExecutionAppendStatus.DUPLICATE;
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
                    tradeApplication);
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

    private static UUID eventId(UUID orderId, String discriminator) {
        return UUID.nameUUIDFromBytes(
                (orderId + ":" + discriminator).getBytes(StandardCharsets.UTF_8));
    }

    private static UUID tradeMatchedEventId(UUID orderId, String tradeId) {
        return eventId(orderId, "TRADE_EXECUTED:" + tradeId);
    }

}
