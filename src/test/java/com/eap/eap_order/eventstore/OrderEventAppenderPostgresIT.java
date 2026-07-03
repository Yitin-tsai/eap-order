package com.eap.eap_order.eventstore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.eap.eap_order.eventstore.OrderEventAppender.TradeExecutionAppendStatus.APPLIED;
import static com.eap.eap_order.eventstore.OrderEventAppender.TradeExecutionAppendStatus.DUPLICATE;
import static com.eap.eap_order.eventstore.OrderEventAppender.TradeExecutionAppendStatus.NOT_FAST_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.datasource.url=jdbc:postgresql://localhost:5432/eapdb",
                "spring.datasource.username=admin",
                "spring.datasource.password=admin123",
                "spring.datasource.driver-class-name=org.postgresql.Driver",
                "spring.jpa.hibernate.ddl-auto=validate",
                "spring.liquibase.enabled=true",
                "spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.xml",
                "spring.liquibase.drop-first=false",
                "spring.rabbitmq.listener.simple.auto-startup=false",
                "eap.wallet.base-url=http://localhost:8081/eap-wallet",
                "eap.matchEngine.base-url=http://localhost:8082/match-engine"
        })
@EnabledIfSystemProperty(named = "eap.integration.postgres", matches = "true")
class OrderEventAppenderPostgresIT {

    @Autowired
    private OrderEventAppender appender;

    @Autowired
    private JdbcTemplate jdbc;

    private final List<UUID> aggregateIds = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        for (UUID aggregateId : aggregateIds) {
            jdbc.update("DELETE FROM order_service.order_event_outbox WHERE aggregate_id = ?", aggregateId);
            jdbc.update("DELETE FROM order_service.order_event_store WHERE aggregate_id = ?", aggregateId);
            jdbc.update("DELETE FROM order_service.order_execution_links WHERE order_id = ?", aggregateId);
            jdbc.update("DELETE FROM order_service.orders_current WHERE order_id = ?", aggregateId);
            jdbc.update("DELETE FROM order_service.order_stream_heads WHERE aggregate_id = ?", aggregateId);
        }
    }

    @Test
    void appendAndRetry_shouldCreateOneEventOneOutboxAndOneHead() {
        UUID aggregateId = aggregateId();
        UUID eventId = UUID.randomUUID();
        OrderEventAppendCommand command = command(
                aggregateId,
                0,
                eventId,
                "OrderSubmissionRequestedV1",
                Map.of("price", 10, "amount", 1),
                new OrderIntegrationEvent("order.exchange", "order.submitted", Map.of("orderId", aggregateId))
        );

        OrderEventAppendResult first = appender.append(command);
        OrderEventAppendResult duplicate = appender.append(command);

        assertFalse(first.duplicate());
        assertEquals(1, first.aggregateVersion());
        assertTrue(duplicate.duplicate());
        assertEquals(first.globalPosition(), duplicate.globalPosition());
        assertEquals(1, count("order_event_store", aggregateId));
        assertEquals(1, count("order_event_outbox", aggregateId));
        assertEquals(1, count("order_stream_heads", aggregateId));
    }

    @Test
    void appendSubsequentEvent_shouldAdvanceVersionAndHashChain() {
        UUID aggregateId = aggregateId();
        OrderEventAppendResult first = appender.append(command(
                aggregateId,
                0,
                UUID.randomUUID(),
                "OrderSubmissionRequestedV1",
                Map.of("status", "PENDING_ASSET_CHECK"),
                null
        ));
        OrderEventAppendResult second = appender.append(command(
                aggregateId,
                1,
                UUID.randomUUID(),
                "OrderAssetReservationConfirmedV1",
                Map.of("status", "OPEN"),
                null
        ));

        assertEquals(2, second.aggregateVersion());
        assertEquals(first.hash(), jdbc.queryForObject("""
                SELECT prev_hash
                FROM order_service.order_event_store
                WHERE aggregate_id = ? AND aggregate_version = 2
                """, String.class, aggregateId));
        assertEquals(2L, jdbc.queryForObject("""
                SELECT current_version
                FROM order_service.order_stream_heads
                WHERE aggregate_id = ?
                """, Long.class, aggregateId));
    }

    @Test
    void staleExpectedVersion_shouldRollbackNewEvent() {
        UUID aggregateId = aggregateId();
        appender.append(command(
                aggregateId, 0, UUID.randomUUID(),
                "OrderSubmissionRequestedV1", Map.of("status", "PENDING_ASSET_CHECK"), null));

        assertThrows(OrderEventVersionConflictException.class, () -> appender.append(command(
                aggregateId, 0, UUID.randomUUID(),
                "OrderAssetReservationConfirmedV1", Map.of("status", "OPEN"), null)));

        assertEquals(1, count("order_event_store", aggregateId));
        assertEquals(1L, jdbc.queryForObject("""
                SELECT current_version FROM order_service.order_stream_heads WHERE aggregate_id = ?
                """, Long.class, aggregateId));
    }

    @Test
    void outboxFailure_shouldRollbackEventAndHeadCreation() {
        UUID aggregateId = aggregateId();
        String tooLongExchange = "x".repeat(101);
        OrderEventAppendCommand command = command(
                aggregateId,
                0,
                UUID.randomUUID(),
                "OrderSubmissionRequestedV1",
                Map.of("status", "PENDING_ASSET_CHECK"),
                new OrderIntegrationEvent(tooLongExchange, "order.submitted", Map.of("orderId", aggregateId))
        );

        assertThrows(DataIntegrityViolationException.class, () -> appender.append(command));

        assertEquals(0, count("order_event_store", aggregateId));
        assertEquals(0, count("order_event_outbox", aggregateId));
        assertEquals(0, count("order_stream_heads", aggregateId));
    }

    @Test
    void concurrentSameExpectedVersion_shouldAllowExactlyOneAppend() throws Exception {
        UUID aggregateId = aggregateId();
        int concurrency = 24;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int i = 0; i < concurrency; i++) {
            int index = i;
            Callable<Boolean> task = () -> {
                ready.countDown();
                start.await();
                try {
                    appender.append(command(
                            aggregateId,
                            0,
                            UUID.randomUUID(),
                            "ConcurrentEventV1",
                            Map.of("worker", index),
                            null
                    ));
                    return true;
                } catch (OrderEventVersionConflictException expected) {
                    return false;
                }
            };
            futures.add(executor.submit(task));
        }

        assertTrue(ready.await(5, TimeUnit.SECONDS));
        start.countDown();
        int successes = 0;
        for (Future<Boolean> future : futures) {
            if (future.get(15, TimeUnit.SECONDS)) {
                successes++;
            }
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(1, successes);
        assertEquals(1, count("order_event_store", aggregateId));
        assertEquals(1L, jdbc.queryForObject("""
                SELECT current_version FROM order_service.order_stream_heads WHERE aggregate_id = ?
                """, Long.class, aggregateId));
    }

    @Test
    void appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent_shouldInsertLinkEventHeadAndOutbox() {
        UUID aggregateId = aggregateId();
        seedCaughtUpProjection(aggregateId, 2, "OPEN", 10);

        OrderEventAppender.TradeExecutionAppendResult result =
                appender.appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent(
                        matchedCommand(aggregateId, "trade-1", "order.trade.applied"),
                        4,
                        tradeLink(aggregateId, "trade-1", 4));

        assertEquals(APPLIED, result.status());
        assertEquals(3, result.appendResult().aggregateVersion());
        assertEquals(1, count("order_execution_links", aggregateId));
        assertEquals(1, count("order_event_store", aggregateId));
        assertEquals(1, count("order_event_outbox", aggregateId));
        assertEquals(3L, currentVersion(aggregateId));
    }

    @Test
    void appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent_duplicateLinkShouldSkipAppend() {
        UUID aggregateId = aggregateId();
        seedCaughtUpProjection(aggregateId, 2, "OPEN", 10);

        OrderEventAppender.TradeExecutionAppendResult first =
                appender.appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent(
                        matchedCommand(aggregateId, "trade-duplicate", "order.trade.applied"),
                        4,
                        tradeLink(aggregateId, "trade-duplicate", 4));
        OrderEventAppender.TradeExecutionAppendResult duplicate =
                appender.appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent(
                        matchedCommand(aggregateId, "trade-duplicate", "order.trade.applied"),
                        4,
                        tradeLink(aggregateId, "trade-duplicate", 4));

        assertEquals(APPLIED, first.status());
        assertEquals(DUPLICATE, duplicate.status());
        assertEquals(1, count("order_execution_links", aggregateId));
        assertEquals(1, count("order_event_store", aggregateId));
        assertEquals(1, count("order_event_outbox", aggregateId));
        assertEquals(3L, currentVersion(aggregateId));
    }

    @Test
    void appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent_staleProjectionShouldStillApplyFromCommandState() {
        UUID aggregateId = aggregateId();
        UUID userId = UUID.randomUUID();
        seedStreamHead(aggregateId, 2, userId, "OPEN", 10);
        seedOrderProjection(aggregateId, 1, userId, "OPEN", 10);

        OrderEventAppender.TradeExecutionAppendResult result =
                appender.appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent(
                        matchedCommand(aggregateId, "trade-stale", "order.trade.applied"),
                        4,
                        tradeLink(aggregateId, "trade-stale", 4));

        assertEquals(APPLIED, result.status());
        assertEquals(1, count("order_execution_links", aggregateId));
        assertEquals(1, count("order_event_store", aggregateId));
        assertEquals(1, count("order_event_outbox", aggregateId));
        assertEquals(3L, currentVersion(aggregateId));
    }

    @Test
    void appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent_cancelledOrFilledProjectionShouldNotInsertLinkOrEvent() {
        for (String status : List.of("CANCELLED", "FILLED")) {
            UUID aggregateId = aggregateId();
            seedCaughtUpProjection(aggregateId, 2, status, 10);

            OrderEventAppender.TradeExecutionAppendResult result =
                    appender.appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent(
                            matchedCommand(aggregateId, "trade-invalid-status-" + status, "order.trade.applied"),
                            4,
                            tradeLink(aggregateId, "trade-invalid-status-" + status, 4));

            assertNotFastPathWithoutWrites(result, aggregateId, 2);
        }
    }

    @Test
    void appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent_insufficientRemainingAmountShouldNotInsertLinkOrEvent() {
        UUID aggregateId = aggregateId();
        seedCaughtUpProjection(aggregateId, 2, "OPEN", 3);

        OrderEventAppender.TradeExecutionAppendResult result =
                appender.appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent(
                        matchedCommand(aggregateId, "trade-insufficient-remaining", "order.trade.applied"),
                        4,
                        tradeLink(aggregateId, "trade-insufficient-remaining", 4));

        assertNotFastPathWithoutWrites(result, aggregateId, 2);
    }

    @Test
    void appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent_nonPositiveMatchedQuantityShouldNotInsertLinkOrEvent() {
        for (int matchedQuantity : List.of(0, -1)) {
            UUID aggregateId = aggregateId();
            seedCaughtUpProjection(aggregateId, 2, "OPEN", 10);

            OrderEventAppender.TradeExecutionAppendResult result =
                    appender.appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent(
                            matchedCommand(aggregateId, "trade-non-positive-" + matchedQuantity, "order.trade.applied"),
                            matchedQuantity,
                            tradeLink(aggregateId, "trade-non-positive-" + matchedQuantity, matchedQuantity));

            assertNotFastPathWithoutWrites(result, aggregateId, 2);
        }
    }

    @Test
    void appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent_outboxFailureShouldRollbackTradeLink() {
        UUID aggregateId = aggregateId();
        seedCaughtUpProjection(aggregateId, 2, "OPEN", 10);

        assertThrows(DataIntegrityViolationException.class, () ->
                appender.appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent(
                        matchedCommand(aggregateId, "trade-rollback", "x".repeat(101)),
                        4,
                        tradeLink(aggregateId, "trade-rollback", 4)));

        assertEquals(0, count("order_execution_links", aggregateId));
        assertEquals(0, count("order_event_store", aggregateId));
        assertEquals(0, count("order_event_outbox", aggregateId));
        assertEquals(2L, currentVersion(aggregateId));
    }

    @Test
    void appendTradeMatchedFromCaughtUpProjectionIfTradeLinksAbsent_shouldAppendBothOrdersAndOneOutbox() {
        UUID buyerOrderId = aggregateId();
        UUID sellerOrderId = aggregateId();
        seedCaughtUpProjection(buyerOrderId, 2, "OPEN", 10);
        seedCaughtUpProjection(sellerOrderId, 2, "OPEN", 10);

        OrderIntegrationEvent sharedMarker = new OrderIntegrationEvent(
                "trade.exchange",
                "trade.order.applied",
                Map.of("tradeId", "trade-pair", "buyerOrderId", buyerOrderId, "sellerOrderId", sellerOrderId));

        OrderEventAppender.TradeExecutionAppendResult result =
                appender.appendTradeMatchedFromCaughtUpProjectionIfTradeLinksAbsent(
                        matchedCommandWithoutOutbox(buyerOrderId, "trade-pair"),
                        4,
                        tradeLink(buyerOrderId, "trade-pair", "BUY", 4),
                        matchedCommandWithoutOutbox(sellerOrderId, "trade-pair"),
                        4,
                        tradeLink(sellerOrderId, "trade-pair", "SELL", 4),
                        sharedMarker);

        assertEquals(APPLIED, result.status());
        assertEquals(1, count("order_event_store", buyerOrderId));
        assertEquals(1, count("order_event_store", sellerOrderId));
        assertEquals(1, count("order_execution_links", buyerOrderId));
        assertEquals(1, count("order_execution_links", sellerOrderId));
        assertEquals(1, count("order_event_outbox", buyerOrderId));
        assertEquals(0, count("order_event_outbox", sellerOrderId));
        assertEquals(3L, currentVersion(buyerOrderId));
        assertEquals(3L, currentVersion(sellerOrderId));
    }

    @Test
    void appendTradeMatchedFromCaughtUpProjectionIfTradeLinksAbsent_duplicateShouldSkipBothOrders() {
        UUID buyerOrderId = aggregateId();
        UUID sellerOrderId = aggregateId();
        seedCaughtUpProjection(buyerOrderId, 2, "OPEN", 10);
        seedCaughtUpProjection(sellerOrderId, 2, "OPEN", 10);
        OrderIntegrationEvent sharedMarker = new OrderIntegrationEvent(
                "trade.exchange",
                "trade.order.applied",
                Map.of("tradeId", "trade-pair-duplicate"));

        OrderEventAppender.TradeExecutionAppendResult first =
                appender.appendTradeMatchedFromCaughtUpProjectionIfTradeLinksAbsent(
                        matchedCommandWithoutOutbox(buyerOrderId, "trade-pair-duplicate"),
                        4,
                        tradeLink(buyerOrderId, "trade-pair-duplicate", "BUY", 4),
                        matchedCommandWithoutOutbox(sellerOrderId, "trade-pair-duplicate"),
                        4,
                        tradeLink(sellerOrderId, "trade-pair-duplicate", "SELL", 4),
                        sharedMarker);
        OrderEventAppender.TradeExecutionAppendResult duplicate =
                appender.appendTradeMatchedFromCaughtUpProjectionIfTradeLinksAbsent(
                        matchedCommandWithoutOutbox(buyerOrderId, "trade-pair-duplicate"),
                        4,
                        tradeLink(buyerOrderId, "trade-pair-duplicate", "BUY", 4),
                        matchedCommandWithoutOutbox(sellerOrderId, "trade-pair-duplicate"),
                        4,
                        tradeLink(sellerOrderId, "trade-pair-duplicate", "SELL", 4),
                        sharedMarker);

        assertEquals(APPLIED, first.status());
        assertEquals(DUPLICATE, duplicate.status());
        assertEquals(1, count("order_event_store", buyerOrderId));
        assertEquals(1, count("order_event_store", sellerOrderId));
        assertEquals(1, count("order_event_outbox", buyerOrderId));
        assertEquals(0, count("order_event_outbox", sellerOrderId));
        assertEquals(3L, currentVersion(buyerOrderId));
        assertEquals(3L, currentVersion(sellerOrderId));
    }

    private UUID aggregateId() {
        UUID id = UUID.randomUUID();
        aggregateIds.add(id);
        return id;
    }

    private OrderEventAppendCommand command(
            UUID aggregateId,
            long expectedVersion,
            UUID eventId,
            String eventType,
            Object payload,
            OrderIntegrationEvent integrationEvent) {
        return new OrderEventAppendCommand(
                aggregateId,
                expectedVersion,
                eventId,
                eventType,
                payload,
                Map.of("correlationId", aggregateId.toString()),
                1,
                LocalDateTime.now(),
                integrationEvent
        );
    }

    private OrderEventAppendCommand matchedCommand(UUID aggregateId, String tradeId, String exchange) {
        return command(
                aggregateId,
                0,
                UUID.nameUUIDFromBytes((aggregateId + ":MATCHED:" + tradeId).getBytes(StandardCharsets.UTF_8)),
                "OrderMatchedV1",
                Map.of("orderId", aggregateId, "matchId", 1001, "amount", 4, "dealPrice", 20),
                new OrderIntegrationEvent(exchange, "trade.order.applied", Map.of("tradeId", tradeId, "orderId", aggregateId))
        );
    }

    private OrderEventAppendCommand matchedCommandWithoutOutbox(UUID aggregateId, String tradeId) {
        return command(
                aggregateId,
                0,
                UUID.nameUUIDFromBytes((aggregateId + ":MATCHED:" + tradeId).getBytes(StandardCharsets.UTF_8)),
                "OrderMatchedV1",
                Map.of("orderId", aggregateId, "matchId", 1001, "amount", 4, "dealPrice", 20),
                null
        );
    }

    private OrderTradeExecutionLink tradeLink(UUID aggregateId, String tradeId, int quantity) {
        return tradeLink(aggregateId, tradeId, "BUY", quantity);
    }

    private OrderTradeExecutionLink tradeLink(UUID aggregateId, String tradeId, String side, int quantity) {
        return new OrderTradeExecutionLink(tradeId, aggregateId, side, 20, quantity, LocalDateTime.now());
    }

    private void seedCaughtUpProjection(UUID aggregateId, long version, String status, int remainingAmount) {
        UUID userId = UUID.randomUUID();
        seedStreamHead(aggregateId, version, userId, status, remainingAmount);
        seedOrderProjection(aggregateId, version, userId, status, remainingAmount);
    }

    private void seedStreamHead(UUID aggregateId, long version, UUID userId, String status, int remainingAmount) {
        jdbc.update("""
                INSERT INTO order_service.order_stream_heads
                    (aggregate_id, current_version, last_hash, user_id, remaining_amount, status, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, aggregateId, version, "a".repeat(64), userId, remainingAmount, status);
    }

    private void seedOrderProjection(UUID aggregateId, long version, UUID userId, String status, int remainingAmount) {
        jdbc.update("""
                INSERT INTO order_service.orders_current
                    (order_id, user_id, market_id, market_sequence, side, price,
                     original_amount, remaining_amount, matched_amount, status,
                     aggregate_version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, aggregateId, userId, "TEST-MARKET", 1L, "BUY", 20,
                10, remainingAmount, 10 - remainingAmount, status, version);
    }

    private void assertNotFastPathWithoutWrites(
            OrderEventAppender.TradeExecutionAppendResult result,
            UUID aggregateId,
            long expectedVersion) {
        assertEquals(NOT_FAST_PATH, result.status());
        assertNull(result.appendResult());
        assertEquals(0, count("order_execution_links", aggregateId));
        assertEquals(0, count("order_event_store", aggregateId));
        assertEquals(0, count("order_event_outbox", aggregateId));
        assertEquals(expectedVersion, currentVersion(aggregateId));
    }

    private int count(String table, UUID aggregateId) {
        String idColumn = "order_execution_links".equals(table) ? "order_id" : "aggregate_id";
        return jdbc.queryForObject(
                "SELECT count(*) FROM order_service." + table + " WHERE " + idColumn + " = ?",
                Integer.class,
                aggregateId
        );
    }

    private long currentVersion(UUID aggregateId) {
        return jdbc.queryForObject("""
                SELECT current_version
                FROM order_service.order_stream_heads
                WHERE aggregate_id = ?
                """, Long.class, aggregateId);
    }
}
