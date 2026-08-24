package com.eap.eap_order.eventstore;

import com.eap.common.event.OrderSubmittedEvent;
import com.eap.common.event.OrderCancellationRequestedEvent;
import com.eap.eap_order.domain.ordersourcing.OrderAssetReservationConfirmedV1;
import com.eap.eap_order.domain.ordersourcing.OrderCancelledV1;
import com.eap.eap_order.domain.ordersourcing.OrderCancellationRequestedV1;
import com.eap.eap_order.domain.ordersourcing.OrderSubmissionRequestedV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HexFormat;
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
import static com.eap.common.constants.RabbitMQConstants.ORDER_CANCELLATION_REQUESTED_KEY;
import static com.eap.common.constants.RabbitMQConstants.ORDER_EXCHANGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
                "eap.scheduling.enabled=false",
                "eap.wallet.base-url=http://localhost:8081/eap-wallet",
                "eap.matchEngine.base-url=http://localhost:8082/match-engine",
                "eap.order.listeners.asset-reservation-confirmed.single-round-trip-enabled=true"
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
            jdbc.update("DELETE FROM order_service.order_trade_applications WHERE buyer_order_id = ? OR seller_order_id = ?",
                    aggregateId, aggregateId);
            jdbc.update("DELETE FROM order_service.orders_current WHERE order_id = ?", aggregateId);
            jdbc.update("DELETE FROM order_service.order_matching_state WHERE order_id = ?", aggregateId);
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
    void appendInitialOrderSubmission_shouldUseFastPathAndCreateCommandStateAndOutbox() {
        UUID aggregateId = aggregateId();
        UUID userId = UUID.randomUUID();
        UUID eventId = UUID.nameUUIDFromBytes(
                (aggregateId + ":REQUESTED").getBytes(StandardCharsets.UTF_8));
        LocalDateTime occurredAt = LocalDateTime.now();
        OrderSubmissionRequestedV1 domainEvent = new OrderSubmissionRequestedV1(
                aggregateId, userId, "ENERGY-SPOT", 123L, "SELL", 10, 3, occurredAt);
        OrderSubmittedEvent integrationEvent = OrderSubmittedEvent.builder()
                .orderId(aggregateId)
                .userId(userId)
                .marketId("ENERGY-SPOT")
                .marketSequence(123L)
                .orderType("SELL")
                .price(10)
                .amount(3)
                .createdAt(occurredAt)
                .build();

        OrderEventAppendResult result = appender.append(new OrderEventAppendCommand(
                aggregateId,
                0,
                eventId,
                "OrderSubmissionRequestedV1",
                domainEvent,
                Map.of("correlationId", aggregateId.toString(), "userId", userId.toString()),
                1,
                occurredAt,
                new OrderIntegrationEvent("order.exchange", "order.submitted", integrationEvent)));

        assertFalse(result.duplicate());
        assertEquals(1, result.aggregateVersion());
        assertEquals(1, count("order_event_store", aggregateId));
        assertEquals(1, count("order_event_outbox", aggregateId));
        assertEquals(1, count("order_stream_heads", aggregateId));
        assertNoMatchingState(aggregateId);
    }

    @Test
    void appendInitialSubmissionFastPath_thenAssetConfirmedBatchShouldCreateMatchingState() {
        UUID aggregateId = aggregateId();
        UUID userId = UUID.randomUUID();
        LocalDateTime occurredAt = LocalDateTime.now();
        OrderSubmissionRequestedV1 domainEvent = new OrderSubmissionRequestedV1(
                aggregateId, userId, "ENERGY-SPOT", 123L, "SELL", 10, 3, occurredAt);
        OrderSubmittedEvent integrationEvent = OrderSubmittedEvent.builder()
                .orderId(aggregateId)
                .userId(userId)
                .marketId("ENERGY-SPOT")
                .marketSequence(123L)
                .orderType("SELL")
                .price(10)
                .amount(3)
                .createdAt(occurredAt)
                .build();
        appender.append(new OrderEventAppendCommand(
                aggregateId,
                0,
                UUID.nameUUIDFromBytes((aggregateId + ":REQUESTED").getBytes(StandardCharsets.UTF_8)),
                "OrderSubmissionRequestedV1",
                domainEvent,
                Map.of("correlationId", aggregateId.toString(), "userId", userId.toString()),
                1,
                occurredAt,
                new OrderIntegrationEvent("order.exchange", "order.submitted", integrationEvent)));

        appender.appendFromConsumerBatch(List.of(command(
                aggregateId,
                1,
                UUID.nameUUIDFromBytes((aggregateId + ":CONFIRMED").getBytes(StandardCharsets.UTF_8)),
                "OrderAssetReservationConfirmedV1",
                new OrderAssetReservationConfirmedV1(aggregateId, userId, LocalDateTime.now()),
                null
        )));

        assertEquals(2L, currentVersion(aggregateId));
        assertMatchingState(aggregateId, 3, 0, "OPEN");
    }

    @Test
    void appendAssetConfirmedSingleRoundTripBatch_shouldPreserveHashChainsAndMatchingState() {
        List<OrderEventAppendResult> initialResults = new ArrayList<>();
        List<OrderEventAppendCommand> confirmedCommands = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            UUID aggregateId = aggregateId();
            UUID userId = UUID.randomUUID();
            initialResults.add(appendInitialSubmission(aggregateId, userId, i + 2));
            confirmedCommands.add(assetReservationConfirmedCommand(aggregateId, userId));
        }

        appender.appendFromConsumerBatch(confirmedCommands);

        for (int i = 0; i < confirmedCommands.size(); i++) {
            OrderEventAppendCommand command = confirmedCommands.get(i);
            OrderEventAppendResult initialResult = initialResults.get(i);
            Map<String, Object> event = jdbc.queryForMap("""
                    SELECT payload_canonical, metadata_canonical, prev_hash, hash
                    FROM order_service.order_event_store
                    WHERE aggregate_id = ? AND aggregate_version = 2
                    """, command.aggregateId());
            String prevHash = (String) event.get("prev_hash");
            assertEquals(initialResult.hash(), prevHash);
            assertEquals(expectedHash(
                            command,
                            (String) event.get("payload_canonical"),
                            (String) event.get("metadata_canonical"),
                            prevHash),
                    event.get("hash"));
            assertEquals(2L, currentVersion(command.aggregateId()));
            assertMatchingState(command.aggregateId(), i + 2, 0, "OPEN");
        }
    }

    @Test
    void appendAssetConfirmedSingleRoundTripBatch_ineligibleBatchShouldPreserveDuplicateFallback() {
        UUID duplicateOrderId = aggregateId();
        UUID duplicateUserId = UUID.randomUUID();
        UUID freshOrderId = aggregateId();
        UUID freshUserId = UUID.randomUUID();
        appendInitialSubmission(duplicateOrderId, duplicateUserId, 3);
        appendInitialSubmission(freshOrderId, freshUserId, 4);
        OrderEventAppendCommand duplicateCommand =
                assetReservationConfirmedCommand(duplicateOrderId, duplicateUserId);
        OrderEventAppendCommand freshCommand =
                assetReservationConfirmedCommand(freshOrderId, freshUserId);

        appender.appendFromConsumerBatch(List.of(duplicateCommand));
        appender.appendFromConsumerBatch(List.of(duplicateCommand, freshCommand));

        assertEquals(2, count("order_event_store", duplicateOrderId));
        assertEquals(2, count("order_event_store", freshOrderId));
        assertEquals(2L, currentVersion(duplicateOrderId));
        assertEquals(2L, currentVersion(freshOrderId));
        assertMatchingState(duplicateOrderId, 3, 0, "OPEN");
        assertMatchingState(freshOrderId, 4, 0, "OPEN");
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
    void appendCancellationRequestIfCurrentStateAllows_openOrderShouldPersistIntentWithoutChangingState() {
        UUID aggregateId = aggregateId();
        UUID userId = UUID.randomUUID();
        seedStreamHead(aggregateId, 2, userId, "OPEN", 10);
        seedMatchingState(aggregateId, userId, "OPEN", 10);

        OrderEventAppendResult result = appender.appendCancellationRequestIfCurrentStateAllows(
                cancellationRequestCommand(aggregateId, userId, LocalDateTime.now()));

        assertFalse(result.duplicate());
        assertEquals(3, result.aggregateVersion());
        assertEquals(1, count("order_event_store", aggregateId));
        assertEquals(3L, currentVersion(aggregateId));
        assertMatchingState(aggregateId, 10, 0, "OPEN");
    }

    @Test
    void appendCancellationRequestIfCurrentStateAllows_matchingProjectionAheadShouldNotRegress() {
        UUID aggregateId = aggregateId();
        UUID userId = UUID.randomUUID();
        seedStreamHead(aggregateId, 2, userId, "OPEN", 10);
        seedMatchingState(aggregateId, userId, "PARTIALLY_MATCHED", 6);

        appender.appendCancellationRequestIfCurrentStateAllows(
                cancellationRequestCommand(aggregateId, userId, LocalDateTime.now()));

        assertMatchingState(aggregateId, 6, 4, "PARTIALLY_MATCHED");
        assertStreamHeadState(aggregateId, 6, "PARTIALLY_MATCHED");
        assertEquals(10, jdbc.queryForObject("""
                SELECT (payload_canonical::jsonb ->> 'originalAmount')::integer
                FROM order_service.order_event_store
                WHERE aggregate_id = ? AND event_type = 'OrderCancellationRequestedV1'
                """, Integer.class, aggregateId));
        assertEquals(10, jdbc.queryForObject("""
                SELECT (payload::jsonb ->> 'originalAmount')::integer
                FROM order_service.order_event_outbox
                WHERE aggregate_id = ? AND routing_key = 'order.cancellation.requested'
                """, Integer.class, aggregateId));
    }

    @Test
    void appendCancellationRequestIfCurrentStateAllows_retryWithNewTimestampShouldBeDuplicate() {
        UUID aggregateId = aggregateId();
        UUID userId = UUID.randomUUID();
        seedStreamHead(aggregateId, 2, userId, "OPEN", 10);
        seedMatchingState(aggregateId, userId, "OPEN", 10);

        OrderEventAppendResult first = appender.appendCancellationRequestIfCurrentStateAllows(
                cancellationRequestCommand(aggregateId, userId, LocalDateTime.now()));
        OrderEventAppendResult retry = appender.appendCancellationRequestIfCurrentStateAllows(
                cancellationRequestCommand(aggregateId, userId, LocalDateTime.now().plusSeconds(1)));

        assertFalse(first.duplicate());
        assertTrue(retry.duplicate());
        assertEquals(first.globalPosition(), retry.globalPosition());
        assertEquals(1, count("order_event_store", aggregateId));
        assertMatchingState(aggregateId, 10, 0, "OPEN");
    }

    @Test
    void appendCancellationRequestIfCurrentStateAllows_retryFromDifferentUserShouldReject() {
        UUID aggregateId = aggregateId();
        UUID ownerId = UUID.randomUUID();
        seedStreamHead(aggregateId, 2, ownerId, "OPEN", 10);
        seedMatchingState(aggregateId, ownerId, "OPEN", 10);

        appender.appendCancellationRequestIfCurrentStateAllows(
                cancellationRequestCommand(aggregateId, ownerId, LocalDateTime.now()));

        assertThrows(IllegalArgumentException.class, () ->
                appender.appendCancellationRequestIfCurrentStateAllows(
                        cancellationRequestCommand(aggregateId, UUID.randomUUID(), LocalDateTime.now().plusSeconds(1))));

        assertEquals(1, count("order_event_store", aggregateId));
        assertMatchingState(aggregateId, 10, 0, "OPEN");
    }

    @Test
    void appendCancellationRequestIfCurrentStateAllows_matchedCommandStateShouldReject() {
        UUID aggregateId = aggregateId();
        UUID userId = UUID.randomUUID();
        seedStreamHead(aggregateId, 2, userId, "OPEN", 10);
        seedMatchingState(aggregateId, userId, "MATCHED", 0);
        seedOrderProjection(aggregateId, 2, userId, "OPEN", 10);

        assertThrows(IllegalStateException.class, () ->
                appender.appendCancellationRequestIfCurrentStateAllows(
                        cancellationRequestCommand(aggregateId, userId, LocalDateTime.now())));

        assertEquals(0, count("order_event_store", aggregateId));
        assertEquals(2L, currentVersion(aggregateId));
        assertMatchingState(aggregateId, 0, 10, "MATCHED");
    }

    @Test
    void appendCancellationRequestIfCurrentStateAllows_wrongUserShouldRejectWithoutAppend() {
        UUID aggregateId = aggregateId();
        UUID ownerId = UUID.randomUUID();
        UUID actorId = UUID.randomUUID();
        seedStreamHead(aggregateId, 2, ownerId, "OPEN", 10);
        seedMatchingState(aggregateId, ownerId, "OPEN", 10);

        assertThrows(IllegalArgumentException.class, () ->
                appender.appendCancellationRequestIfCurrentStateAllows(
                        cancellationRequestCommand(aggregateId, actorId, LocalDateTime.now())));

        assertEquals(0, count("order_event_store", aggregateId));
        assertEquals(2L, currentVersion(aggregateId));
        assertMatchingState(aggregateId, 10, 0, "OPEN");
    }

    @Test
    void appendCancellationAcceptedIfReady_exactRemainderShouldCancel() {
        UUID aggregateId = aggregateId();
        UUID userId = UUID.randomUUID();
        seedStreamHead(aggregateId, 2, userId, "OPEN", 10);
        seedMatchingState(aggregateId, userId, "PARTIALLY_MATCHED", 6);
        OrderEventAppendCommand request = cancellationRequestCommand(
                aggregateId, userId, LocalDateTime.now().minusSeconds(1));
        appender.appendCancellationRequestIfCurrentStateAllows(request);

        OrderEventAppendResult result = appender.appendCancellationAcceptedIfReady(
                cancellationAcceptedCommand(aggregateId, userId, LocalDateTime.now()),
                request.eventId(),
                6);

        assertFalse(result.duplicate());
        assertEquals(4, result.aggregateVersion());
        assertMatchingState(aggregateId, 6, 4, "CANCELLED");
        assertStreamHeadState(aggregateId, 6, "CANCELLED");
    }

    @Test
    void appendCancellationAcceptedIfReady_tradeProjectionBehindShouldWait() {
        UUID aggregateId = aggregateId();
        UUID userId = UUID.randomUUID();
        seedStreamHead(aggregateId, 2, userId, "OPEN", 10);
        seedMatchingState(aggregateId, userId, "OPEN", 10);
        OrderEventAppendCommand request = cancellationRequestCommand(
                aggregateId, userId, LocalDateTime.now().minusSeconds(1));
        appender.appendCancellationRequestIfCurrentStateAllows(request);

        assertThrows(CancellationPrerequisiteNotReadyException.class, () ->
                appender.appendCancellationAcceptedIfReady(
                        cancellationAcceptedCommand(aggregateId, userId, LocalDateTime.now()),
                        request.eventId(),
                        6));

        assertEquals(1, count("order_event_store", aggregateId));
        assertMatchingState(aggregateId, 10, 0, "OPEN");
    }

    @Test
    void appendCancellationAcceptedIfReady_withoutPersistedRequestShouldReject() {
        UUID aggregateId = aggregateId();
        UUID userId = UUID.randomUUID();
        seedStreamHead(aggregateId, 2, userId, "OPEN", 6);
        seedMatchingState(aggregateId, userId, "OPEN", 6);

        assertThrows(IllegalStateException.class, () ->
                appender.appendCancellationAcceptedIfReady(
                        cancellationAcceptedCommand(aggregateId, userId, LocalDateTime.now()),
                        UUID.randomUUID(),
                        6));

        assertEquals(0, count("order_event_store", aggregateId));
        assertMatchingState(aggregateId, 6, 4, "OPEN");
    }

    @Test
    void appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsent_shouldApplyTradeWithoutOutbox() {
        UUID buyerOrderId = aggregateId();
        UUID sellerOrderId = aggregateId();
        LocalDateTime appliedAt = LocalDateTime.now().minusMinutes(1);
        seedCaughtUpProjection(buyerOrderId, 2, "OPEN", 10);
        seedCaughtUpProjection(sellerOrderId, 2, "OPEN", 10);

        OrderEventAppender.TradeExecutionAppendResult result =
                appender.appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsent(
                        matchedCommandWithoutOutbox(buyerOrderId, "trade-application"),
                        4,
                        matchedCommandWithoutOutbox(sellerOrderId, "trade-application"),
                        4,
                        tradeApplication("trade-application", buyerOrderId, sellerOrderId, 4, appliedAt));

        assertEquals(APPLIED, result.status());
        assertEquals(0, count("order_event_store", buyerOrderId));
        assertEquals(0, count("order_event_store", sellerOrderId));
        assertEquals(1, countTradeApplications("trade-application"));
        assertEquals(0, count("order_event_outbox", buyerOrderId));
        assertEquals(0, count("order_event_outbox", sellerOrderId));
        LocalDateTime insertedAt = jdbc.queryForObject(
                "SELECT inserted_at FROM order_service.order_trade_applications WHERE trade_id = ?",
                LocalDateTime.class,
                "trade-application");
        assertTrue(insertedAt.isAfter(appliedAt));
        assertEquals(2L, currentVersion(buyerOrderId));
        assertEquals(2L, currentVersion(sellerOrderId));
        assertMatchingState(buyerOrderId, 6, 4, "PARTIALLY_MATCHED");
        assertMatchingState(sellerOrderId, 6, 4, "PARTIALLY_MATCHED");
    }

    @Test
    void appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsent_duplicateShouldSkipBothOrders() {
        UUID buyerOrderId = aggregateId();
        UUID sellerOrderId = aggregateId();
        LocalDateTime appliedAt = LocalDateTime.now();
        seedCaughtUpProjection(buyerOrderId, 2, "OPEN", 10);
        seedCaughtUpProjection(sellerOrderId, 2, "OPEN", 10);

        OrderTradeApplication tradeApplication =
                tradeApplication("trade-application-duplicate", buyerOrderId, sellerOrderId, 4, appliedAt);
        OrderEventAppender.TradeExecutionAppendResult first =
                appender.appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsent(
                        matchedCommandWithoutOutbox(buyerOrderId, "trade-application-duplicate"),
                        4,
                        matchedCommandWithoutOutbox(sellerOrderId, "trade-application-duplicate"),
                        4,
                        tradeApplication);
        OrderEventAppender.TradeExecutionAppendResult duplicate =
                appender.appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsent(
                        matchedCommandWithoutOutbox(buyerOrderId, "trade-application-duplicate"),
                        4,
                        matchedCommandWithoutOutbox(sellerOrderId, "trade-application-duplicate"),
                        4,
                        tradeApplication);

        assertEquals(APPLIED, first.status());
        assertEquals(DUPLICATE, duplicate.status());
        assertEquals(0, count("order_event_store", buyerOrderId));
        assertEquals(0, count("order_event_store", sellerOrderId));
        assertEquals(1, countTradeApplications("trade-application-duplicate"));
        assertEquals(0, count("order_event_outbox", buyerOrderId));
        assertEquals(0, count("order_event_outbox", sellerOrderId));
        assertEquals(2L, currentVersion(buyerOrderId));
        assertEquals(2L, currentVersion(sellerOrderId));
        assertMatchingState(buyerOrderId, 6, 4, "PARTIALLY_MATCHED");
        assertMatchingState(sellerOrderId, 6, 4, "PARTIALLY_MATCHED");
    }

    @Test
    void appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsent_fullyMatchedDuplicateShouldSkipBothOrders() {
        UUID buyerOrderId = aggregateId();
        UUID sellerOrderId = aggregateId();
        LocalDateTime appliedAt = LocalDateTime.now();
        seedCaughtUpProjection(buyerOrderId, 2, "OPEN", 10);
        seedCaughtUpProjection(sellerOrderId, 2, "OPEN", 10);

        OrderTradeApplication tradeApplication =
                tradeApplication("fully-matched-trade-duplicate", buyerOrderId, sellerOrderId, 10, appliedAt);
        OrderEventAppender.TradeExecutionAppendResult first =
                appender.appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsent(
                        matchedCommandWithoutOutbox(buyerOrderId, "fully-matched-trade-duplicate"),
                        10,
                        matchedCommandWithoutOutbox(sellerOrderId, "fully-matched-trade-duplicate"),
                        10,
                        tradeApplication);
        OrderEventAppender.TradeExecutionAppendResult duplicate =
                appender.appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsent(
                        matchedCommandWithoutOutbox(buyerOrderId, "fully-matched-trade-duplicate"),
                        10,
                        matchedCommandWithoutOutbox(sellerOrderId, "fully-matched-trade-duplicate"),
                        10,
                        tradeApplication);

        assertEquals(APPLIED, first.status());
        assertEquals(DUPLICATE, duplicate.status());
        assertEquals(1, countTradeApplications("fully-matched-trade-duplicate"));
        assertMatchingState(buyerOrderId, 0, 10, "MATCHED");
        assertMatchingState(sellerOrderId, 0, 10, "MATCHED");
    }

    @Test
    void appendTradeMatchedBatchFromCaughtUpProjectionIfTradeApplicationsAbsent_shouldAppendNonOverlappingTrades() {
        UUID buyerOrderId1 = aggregateId();
        UUID sellerOrderId1 = aggregateId();
        UUID buyerOrderId2 = aggregateId();
        UUID sellerOrderId2 = aggregateId();
        LocalDateTime appliedAt = LocalDateTime.now();
        for (UUID orderId : List.of(buyerOrderId1, sellerOrderId1, buyerOrderId2, sellerOrderId2)) {
            seedCaughtUpProjection(orderId, 2, "OPEN", 10);
        }

        OrderEventAppender.TradeApplicationBatchAppendResult result =
                appender.appendTradeMatchedBatchFromCaughtUpProjectionIfTradeApplicationsAbsent(List.of(
                        tradeApplicationBatchCommand("trade-batch-1", buyerOrderId1, sellerOrderId1, appliedAt),
                        tradeApplicationBatchCommand("trade-batch-2", buyerOrderId2, sellerOrderId2, appliedAt)));

        assertEquals(OrderEventAppender.TradeApplicationBatchAppendStatus.APPLIED, result.status());
        assertEquals(2, result.appliedCount());
        for (UUID orderId : List.of(buyerOrderId1, sellerOrderId1, buyerOrderId2, sellerOrderId2)) {
            assertEquals(0, count("order_event_store", orderId));
            assertEquals(2L, currentVersion(orderId));
            assertMatchingState(orderId, 6, 4, "PARTIALLY_MATCHED");
        }
        assertEquals(1, countTradeApplications("trade-batch-1"));
        assertEquals(1, countTradeApplications("trade-batch-2"));
        assertEquals(0, count("order_event_outbox", buyerOrderId1));
        assertEquals(0, count("order_event_outbox", sellerOrderId1));
        assertEquals(0, count("order_event_outbox", buyerOrderId2));
        assertEquals(0, count("order_event_outbox", sellerOrderId2));
    }

    @Test
    void appendTradeMatchedBatchFromCaughtUpProjectionIfTradeApplicationsAbsent_overlappingOrderShouldNotWrite() {
        UUID sharedOrderId = aggregateId();
        UUID sellerOrderId1 = aggregateId();
        UUID sellerOrderId2 = aggregateId();
        LocalDateTime appliedAt = LocalDateTime.now();
        for (UUID orderId : List.of(sharedOrderId, sellerOrderId1, sellerOrderId2)) {
            seedCaughtUpProjection(orderId, 2, "OPEN", 10);
        }

        OrderEventAppender.TradeApplicationBatchAppendResult result =
                appender.appendTradeMatchedBatchFromCaughtUpProjectionIfTradeApplicationsAbsent(List.of(
                        tradeApplicationBatchCommand("trade-batch-overlap-1", sharedOrderId, sellerOrderId1, appliedAt),
                        tradeApplicationBatchCommand("trade-batch-overlap-2", sharedOrderId, sellerOrderId2, appliedAt)));

        assertEquals(OrderEventAppender.TradeApplicationBatchAppendStatus.NOT_BATCHABLE, result.status());
        assertEquals(0, result.appliedCount());
        for (UUID orderId : List.of(sharedOrderId, sellerOrderId1, sellerOrderId2)) {
            assertEquals(0, count("order_event_store", orderId));
            assertEquals(2L, currentVersion(orderId));
        }
        assertEquals(0, countTradeApplications("trade-batch-overlap-1"));
        assertEquals(0, countTradeApplications("trade-batch-overlap-2"));
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

    private OrderEventAppendResult appendInitialSubmission(
            UUID aggregateId,
            UUID userId,
            int amount) {
        LocalDateTime occurredAt = LocalDateTime.now();
        OrderSubmissionRequestedV1 domainEvent = new OrderSubmissionRequestedV1(
                aggregateId, userId, "ENERGY-SPOT", 123L, "SELL", 10, amount, occurredAt);
        OrderSubmittedEvent integrationEvent = OrderSubmittedEvent.builder()
                .orderId(aggregateId)
                .userId(userId)
                .marketId("ENERGY-SPOT")
                .marketSequence(123L)
                .orderType("SELL")
                .price(10)
                .amount(amount)
                .createdAt(occurredAt)
                .build();
        return appender.append(new OrderEventAppendCommand(
                aggregateId,
                0,
                UUID.nameUUIDFromBytes((aggregateId + ":REQUESTED").getBytes(StandardCharsets.UTF_8)),
                "OrderSubmissionRequestedV1",
                domainEvent,
                Map.of("correlationId", aggregateId.toString(), "userId", userId.toString()),
                1,
                occurredAt,
                new OrderIntegrationEvent("order.exchange", "order.submitted", integrationEvent)));
    }

    private OrderEventAppendCommand assetReservationConfirmedCommand(
            UUID aggregateId,
            UUID userId) {
        LocalDateTime occurredAt = LocalDateTime.now();
        return new OrderEventAppendCommand(
                aggregateId,
                1,
                UUID.nameUUIDFromBytes((aggregateId + ":CONFIRMED").getBytes(StandardCharsets.UTF_8)),
                "OrderAssetReservationConfirmedV1",
                new OrderAssetReservationConfirmedV1(aggregateId, userId, occurredAt),
                Map.of("correlationId", aggregateId.toString(), "userId", userId.toString()),
                1,
                occurredAt,
                null);
    }

    private String expectedHash(
            OrderEventAppendCommand command,
            String payloadCanonical,
            String metadataCanonical,
            String prevHash) {
        String material = command.eventId() + "|"
                + command.aggregateId() + "|"
                + (command.expectedVersion() + 1) + "|"
                + command.eventType() + "|"
                + payloadCanonical + "|"
                + metadataCanonical + "|"
                + command.schemaVersion() + "|"
                + command.occurredAt() + "|"
                + prevHash;
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256")
                            .digest(material.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("Could not compute expected SHA-256 event hash", e);
        }
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

    private OrderEventAppendCommand cancellationRequestCommand(
            UUID aggregateId,
            UUID userId,
            LocalDateTime requestedAt) {
        UUID cancellationId = UUID.nameUUIDFromBytes(
                (aggregateId + ":CANCELLATION_REQUESTED").getBytes(StandardCharsets.UTF_8));
        return new OrderEventAppendCommand(
                aggregateId,
                0,
                cancellationId,
                "OrderCancellationRequestedV1",
                new OrderCancellationRequestedV1(
                        cancellationId, aggregateId, userId, null, requestedAt),
                Map.of("correlationId", aggregateId.toString(), "userId", userId.toString()),
                1,
                requestedAt,
                new OrderIntegrationEvent(
                        ORDER_EXCHANGE,
                        ORDER_CANCELLATION_REQUESTED_KEY,
                        OrderCancellationRequestedEvent.builder()
                                .cancellationId(cancellationId)
                                .orderId(aggregateId)
                                .userId(userId)
                                .requestedAt(requestedAt)
                                .build())
        );
    }

    private OrderEventAppendCommand cancellationAcceptedCommand(
            UUID aggregateId,
            UUID userId,
            LocalDateTime cancelledAt) {
        return new OrderEventAppendCommand(
                aggregateId,
                0,
                UUID.nameUUIDFromBytes(
                        (aggregateId + ":CANCELLATION_ACCEPTED").getBytes(StandardCharsets.UTF_8)),
                "OrderCancelledV1",
                new OrderCancelledV1(aggregateId, userId, cancelledAt),
                Map.of("correlationId", aggregateId.toString(), "userId", userId.toString()),
                1,
                cancelledAt,
                null);
    }

    private OrderTradeApplication tradeApplication(
            String tradeId,
            UUID buyerOrderId,
            UUID sellerOrderId,
            int quantity,
            LocalDateTime appliedAt) {
        return new OrderTradeApplication(tradeId, buyerOrderId, sellerOrderId, 20, quantity, appliedAt);
    }

    private OrderEventAppender.TradeApplicationBatchAppendCommand tradeApplicationBatchCommand(
            String tradeId,
            UUID buyerOrderId,
            UUID sellerOrderId,
            LocalDateTime appliedAt) {
        return new OrderEventAppender.TradeApplicationBatchAppendCommand(
                matchedCommandWithoutOutbox(buyerOrderId, tradeId),
                4,
                matchedCommandWithoutOutbox(sellerOrderId, tradeId),
                4,
                tradeApplication(tradeId, buyerOrderId, sellerOrderId, 4, appliedAt));
    }

    private void seedCaughtUpProjection(UUID aggregateId, long version, String status, int remainingAmount) {
        UUID userId = UUID.randomUUID();
        seedStreamHead(aggregateId, version, userId, status, remainingAmount);
        seedMatchingState(aggregateId, userId, status, remainingAmount);
        seedOrderProjection(aggregateId, version, userId, status, remainingAmount);
    }

    private void seedStreamHead(UUID aggregateId, long version, UUID userId, String status, int remainingAmount) {
        jdbc.update("""
                INSERT INTO order_service.order_stream_heads
                    (aggregate_id, current_version, last_hash, user_id, remaining_amount, status, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, aggregateId, version, "a".repeat(64), userId, remainingAmount, status);
    }

    private void seedMatchingState(UUID aggregateId, UUID userId, String status, int remainingAmount) {
        jdbc.update("""
                INSERT INTO order_service.order_matching_state
                    (order_id, user_id, remaining_amount, matched_amount, status, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, aggregateId, userId, remainingAmount, 10 - remainingAmount, status);
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

    private int count(String table, UUID aggregateId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM order_service." + table + " WHERE aggregate_id = ?",
                Integer.class,
                aggregateId
        );
    }

    private int countTradeApplications(String tradeId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM order_service.order_trade_applications WHERE trade_id = ?",
                Integer.class,
                tradeId
        );
    }

    private long currentVersion(UUID aggregateId) {
        return jdbc.queryForObject("""
                SELECT current_version
                FROM order_service.order_stream_heads
                WHERE aggregate_id = ?
                """, Long.class, aggregateId);
    }

    private void assertMatchingState(UUID orderId, int remainingAmount, int matchedAmount, String status) {
        Map<String, Object> row = jdbc.queryForMap("""
                SELECT remaining_amount, matched_amount, status
                FROM order_service.order_matching_state
                WHERE order_id = ?
                """, orderId);
        assertEquals(remainingAmount, ((Number) row.get("remaining_amount")).intValue());
        assertEquals(matchedAmount, ((Number) row.get("matched_amount")).intValue());
        assertEquals(status, row.get("status"));
    }

    private void assertStreamHeadState(UUID orderId, int remainingAmount, String status) {
        Map<String, Object> row = jdbc.queryForMap("""
                SELECT remaining_amount, status
                FROM order_service.order_stream_heads
                WHERE aggregate_id = ?
                """, orderId);
        assertEquals(remainingAmount, ((Number) row.get("remaining_amount")).intValue());
        assertEquals(status, row.get("status"));
    }

    private void assertNoMatchingState(UUID orderId) {
        Integer count = jdbc.queryForObject("""
                SELECT count(*)
                FROM order_service.order_matching_state
                WHERE order_id = ?
                """, Integer.class, orderId);
        assertEquals(0, count);
    }
}
