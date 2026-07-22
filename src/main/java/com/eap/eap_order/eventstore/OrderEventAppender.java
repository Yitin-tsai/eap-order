package com.eap.eap_order.eventstore;

import com.eap.eap_order.domain.ordersourcing.OrderAssetReservationConfirmedV1;
import com.eap.eap_order.domain.ordersourcing.OrderAssetReservationFailedV1;
import com.eap.eap_order.domain.ordersourcing.OrderCancelledV1;
import com.eap.eap_order.domain.ordersourcing.OrderMatchedV1;
import com.eap.eap_order.domain.ordersourcing.OrderSubmissionRequestedV1;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Component
public class OrderEventAppender {

    private static final String AGGREGATE_TYPE = "Order";
    private static final String GENESIS_HASH = "0".repeat(64);
    private static final String INSERT_TRADE_APPLICATIONS_MATCHING_STATES_AND_OUTBOXES_SQL = """
            WITH input(trade_id, trade_buyer_order_id, trade_seller_order_id, trade_price,
                       trade_quantity, trade_applied_at,
                       buyer_order_id, buyer_quantity, buyer_previous_remaining_amount,
                       buyer_remaining_amount, buyer_matched_amount, buyer_status,
                       seller_order_id, seller_quantity, seller_previous_remaining_amount,
                       seller_remaining_amount, seller_matched_amount, seller_status,
                       outbox_event_id, outbox_aggregate_id, outbox_exchange, outbox_routing_key,
                       outbox_message_type, outbox_payload) AS (
                SELECT *
                FROM unnest(?::varchar[], ?::uuid[], ?::uuid[], ?::integer[],
                            ?::integer[], ?::timestamp[],
                            ?::uuid[], ?::integer[], ?::integer[],
                            ?::integer[], ?::integer[], ?::varchar[],
                            ?::uuid[], ?::integer[], ?::integer[],
                            ?::integer[], ?::integer[], ?::varchar[],
                            ?::uuid[], ?::uuid[], ?::varchar[], ?::varchar[],
                            ?::varchar[], ?::text[])
            ),
            existing_trade_applications AS (
                SELECT COUNT(*) AS count
                FROM order_service.order_trade_applications existing
                JOIN input ON input.trade_id = existing.trade_id
            ),
            trade_application AS (
                INSERT INTO order_service.order_trade_applications
                    (trade_id, buyer_order_id, seller_order_id, price, quantity, applied_at)
                SELECT trade_id, trade_buyer_order_id, trade_seller_order_id,
                       trade_price, trade_quantity, trade_applied_at
                FROM input
                WHERE (SELECT count FROM existing_trade_applications) = 0
                ON CONFLICT (trade_id) DO NOTHING
                RETURNING trade_id
            ),
            matching_input AS (
                SELECT input.buyer_order_id AS order_id,
                       input.buyer_quantity AS quantity,
                       input.buyer_previous_remaining_amount AS previous_remaining_amount,
                       input.buyer_remaining_amount AS remaining_amount,
                       input.buyer_matched_amount AS matched_amount,
                       input.buyer_status AS order_status,
                       input.trade_id AS trade_id
                FROM input
                JOIN trade_application ON trade_application.trade_id = input.trade_id
                UNION ALL
                SELECT input.seller_order_id AS order_id,
                       input.seller_quantity AS quantity,
                       input.seller_previous_remaining_amount AS previous_remaining_amount,
                       input.seller_remaining_amount AS remaining_amount,
                       input.seller_matched_amount AS matched_amount,
                       input.seller_status AS order_status,
                       input.trade_id AS trade_id
                FROM input
                JOIN trade_application ON trade_application.trade_id = input.trade_id
            ),
            updated_matching_states AS (
                UPDATE order_service.order_matching_state state
                SET remaining_amount = matching_input.remaining_amount,
                    matched_amount = matching_input.matched_amount,
                    status = matching_input.order_status,
                    last_trade_id = matching_input.trade_id,
                    updated_at = CURRENT_TIMESTAMP
                FROM matching_input
                WHERE state.order_id = matching_input.order_id
                  AND state.remaining_amount = matching_input.previous_remaining_amount
                  AND state.status IN ('OPEN', 'PARTIALLY_MATCHED')
                  AND state.remaining_amount >= matching_input.quantity
                RETURNING 1
            ),
            inserted_outbox AS (
                INSERT INTO order_service.order_event_outbox
                    (event_id, aggregate_id, exchange_name, routing_key, message_type, payload,
                     status, attempt_count, next_retry_at, created_at, updated_at)
                SELECT outbox_event_id, outbox_aggregate_id, outbox_exchange, outbox_routing_key,
                       outbox_message_type, outbox_payload,
                       'PENDING', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                FROM input
                JOIN trade_application ON trade_application.trade_id = input.trade_id
                WHERE outbox_event_id IS NOT NULL
                RETURNING 1
            )
            SELECT
                (SELECT count FROM existing_trade_applications) AS existing_trade_applications,
                (SELECT COUNT(*) FROM trade_application) AS inserted_trade_applications,
                (SELECT COUNT(*) FROM updated_matching_states) AS updated_matching_states,
                (SELECT COUNT(*) FROM inserted_outbox) AS inserted_outboxes
            """;

    private final NamedParameterJdbcTemplate commandJdbc;
    private final NamedParameterJdbcTemplate consumerJdbc;
    private final TransactionTemplate commandTransactionTemplate;
    private final TransactionTemplate consumerTransactionTemplate;
    private final ObjectMapper canonicalObjectMapper;
    private final OrderTradeApplyMetrics tradeApplyMetrics;

    public OrderEventAppender(
            @Qualifier("namedParameterJdbcTemplate") NamedParameterJdbcTemplate commandJdbc,
            @Qualifier("orderConsumerNamedParameterJdbcTemplate") NamedParameterJdbcTemplate consumerJdbc,
            @Qualifier("transactionManager") PlatformTransactionManager transactionManager,
            @Qualifier("orderConsumerTransactionManager") PlatformTransactionManager consumerTransactionManager,
            ObjectMapper objectMapper,
            OrderTradeApplyMetrics tradeApplyMetrics) {
        this.commandJdbc = commandJdbc;
        this.consumerJdbc = consumerJdbc;
        this.commandTransactionTemplate = new TransactionTemplate(transactionManager);
        this.consumerTransactionTemplate = new TransactionTemplate(consumerTransactionManager);
        this.tradeApplyMetrics = tradeApplyMetrics;
        this.canonicalObjectMapper = objectMapper.copy()
                .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    public OrderEventAppendResult append(OrderEventAppendCommand command) {
        return commandTransactionTemplate.execute(status -> appendInTransaction(command, commandJdbc));
    }

    public OrderEventAppendResult appendFromConsumer(OrderEventAppendCommand command) {
        return consumerTransactionTemplate.execute(status -> appendInTransaction(command, consumerJdbc));
    }

    public OrderEventAppendResult appendCancellationIfCurrentStateAllows(OrderEventAppendCommand command) {
        return commandTransactionTemplate.execute(status ->
                appendCancellationIfCurrentStateAllowsInTransaction(command));
    }

    public void assertCancellationAllowed(UUID orderId, UUID userId) {
        commandTransactionTemplate.executeWithoutResult(status ->
                assertCancellationAllowedInTransaction(orderId, userId));
    }

    public TradeExecutionAppendResult appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsent(
            OrderEventAppendCommand buyerCommand,
            int buyerMatchedQuantity,
            OrderEventAppendCommand sellerCommand,
            int sellerMatchedQuantity,
            OrderTradeApplication tradeApplication,
            OrderIntegrationEvent integrationEvent) {
        String integrationPayload = integrationEvent == null ? null : serialize(integrationEvent.payload());
        long startedNanos = System.nanoTime();
        try {
            return consumerTransactionTemplate.execute(status ->
                    appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsentInTransaction(
                            buyerCommand, buyerMatchedQuantity,
                            sellerCommand, sellerMatchedQuantity,
                            tradeApplication,
                            integrationEvent,
                            integrationPayload));
        } finally {
            tradeApplyMetrics.record("total", startedNanos);
        }
    }

    public TradeApplicationBatchAppendResult appendTradeMatchedBatchFromCaughtUpProjectionIfTradeApplicationsAbsent(
            List<TradeApplicationBatchAppendCommand> commands) {
        if (commands == null || commands.isEmpty()) {
            return TradeApplicationBatchAppendResult.applied(0);
        }
        List<TradeApplicationBatchAppendCommandWithPayload> commandsWithPayload = commands.stream()
                .map(command -> new TradeApplicationBatchAppendCommandWithPayload(
                        command,
                        command.integrationEvent() == null ? null : serialize(command.integrationEvent().payload())))
                .toList();
        long startedNanos = System.nanoTime();
        try {
            return consumerTransactionTemplate.execute(status ->
                    appendTradeMatchedBatchFromCaughtUpProjectionIfTradeApplicationsAbsentInTransaction(
                            commandsWithPayload));
        } finally {
            tradeApplyMetrics.record("batch_total", startedNanos);
        }
    }

    private OrderEventAppendResult appendInTransaction(
            OrderEventAppendCommand command,
            NamedParameterJdbcTemplate jdbc) {
        if (command.expectedVersion() == 0) {
            createHeadIfAbsent(jdbc, command.aggregateId());
        }
        StreamHead head = lockHead(jdbc, command.aggregateId());
        return appendInTransactionWithLockedHead(command, jdbc, head);
    }

    private OrderEventAppendResult appendCancellationIfCurrentStateAllowsInTransaction(
            OrderEventAppendCommand draftCommand) {
        if (!(draftCommand.payload() instanceof OrderCancelledV1 cancelled)) {
            throw new IllegalArgumentException("Cancellation command must contain OrderCancelledV1 payload");
        }
        assertCancellationAllowedInTransaction(draftCommand.aggregateId(), cancelled.userId());

        StreamHead head = lockHead(commandJdbc, draftCommand.aggregateId());
        OrderEventAppendCommand command = new OrderEventAppendCommand(
                draftCommand.aggregateId(),
                head.currentVersion(),
                draftCommand.eventId(),
                draftCommand.eventType(),
                draftCommand.payload(),
                draftCommand.metadata(),
                draftCommand.schemaVersion(),
                draftCommand.occurredAt(),
                draftCommand.integrationEvent());
        return appendInTransactionWithLockedHead(command, commandJdbc, head);
    }

    private void assertCancellationAllowedInTransaction(UUID orderId, UUID actorUserId) {
        MatchingState state = lockMatchingState(commandJdbc, orderId);
        if (state == null) {
            throw new IllegalStateException("Order command state not found: orderId=" + orderId);
        }
        if (!actorUserId.equals(state.userId())) {
            throw new IllegalArgumentException("Only the order owner can cancel this order");
        }
        if (!state.canCancel()) {
            throw new IllegalStateException("Cannot cancel order in status " + state.status()
                    + " with remainingAmount=" + state.remainingAmount());
        }
    }

    private TradeExecutionAppendResult appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsentInTransaction(
            OrderEventAppendCommand buyerDraftCommand,
            int buyerMatchedQuantity,
            OrderEventAppendCommand sellerDraftCommand,
            int sellerMatchedQuantity,
            OrderTradeApplication tradeApplication,
            OrderIntegrationEvent integrationEvent,
            String integrationPayload) {
        validateDistinctTradeOrders(buyerDraftCommand, sellerDraftCommand);
        long lockStartedNanos = System.nanoTime();
        Map<UUID, MatchingState> states = lockMatchingStatesInStableOrder(
                buyerDraftCommand.aggregateId(), sellerDraftCommand.aggregateId());
        tradeApplyMetrics.record("lock_heads", lockStartedNanos);
        MatchingState buyerState = states.get(buyerDraftCommand.aggregateId());
        MatchingState sellerState = states.get(sellerDraftCommand.aggregateId());
        if (buyerState == null || sellerState == null
                || !buyerState.canMatch(buyerMatchedQuantity)
                || !sellerState.canMatch(sellerMatchedQuantity)) {
            return TradeExecutionAppendResult.notFastPath();
        }
        long appendStartedNanos = System.nanoTime();
        TradeApplicationHotPathOutcome outcome = insertTradeApplicationMatchingStateAndOutbox(
                buyerDraftCommand.aggregateId(),
                buyerMatchedQuantity,
                buyerState,
                sellerDraftCommand.aggregateId(),
                sellerMatchedQuantity,
                sellerState,
                tradeApplication,
                integrationEvent,
                integrationPayload);
        tradeApplyMetrics.record("append_cte", appendStartedNanos);
        if (outcome.insertedTradeApplications() == 0) {
            if (existingTradeApplicationMatches(tradeApplication)) {
                return TradeExecutionAppendResult.duplicate();
            }
            throw new IllegalStateException("Trade application identity conflict: tradeId=" + tradeApplication.tradeId());
        }
        if (outcome.updatedMatchingStates() != 2) {
            throw new IllegalStateException("Expected two order matching states to be updated, actual="
                    + outcome.updatedMatchingStates());
        }
        if (integrationEvent != null && outcome.insertedOutboxes() != 1) {
            throw new IllegalStateException("Expected one shared order outbox row, actual="
                    + outcome.insertedOutboxes());
        }
        OrderEventAppendResult buyerResult = new OrderEventAppendResult(
                buyerDraftCommand.aggregateId(),
                buyerDraftCommand.eventId(),
                0,
                0,
                "",
                false);
        return TradeExecutionAppendResult.applied(buyerResult);
    }

    private TradeApplicationBatchAppendResult appendTradeMatchedBatchFromCaughtUpProjectionIfTradeApplicationsAbsentInTransaction(
            List<TradeApplicationBatchAppendCommandWithPayload> commandsWithPayload) {
        Set<UUID> aggregateIds = new HashSet<>(commandsWithPayload.size() * 2);
        for (TradeApplicationBatchAppendCommandWithPayload commandWithPayload : commandsWithPayload) {
            TradeApplicationBatchAppendCommand command = commandWithPayload.command();
            validateDistinctTradeOrders(command.buyerCommand(), command.sellerCommand());
            if (!aggregateIds.add(command.buyerCommand().aggregateId())
                    || !aggregateIds.add(command.sellerCommand().aggregateId())) {
                return TradeApplicationBatchAppendResult.notBatchable(
                        TradeApplicationBatchNotBatchableReason.OVERLAPPING_ORDER);
            }
        }
        long lockStartedNanos = System.nanoTime();
        Map<UUID, MatchingState> states = lockMatchingStatesInStableOrder(aggregateIds);
        tradeApplyMetrics.record("batch_lock_heads", lockStartedNanos);
        if (states.size() != aggregateIds.size()) {
            return TradeApplicationBatchAppendResult.notBatchable(
                    TradeApplicationBatchNotBatchableReason.MISSING_HEAD);
        }
        long prepareStartedNanos = System.nanoTime();
        List<TradeApplicationHotPathBatchAppend> preparedBatch = new ArrayList<>(commandsWithPayload.size());
        for (TradeApplicationBatchAppendCommandWithPayload commandWithPayload : commandsWithPayload) {
            TradeApplicationBatchAppendCommand command = commandWithPayload.command();
            MatchingState buyerState = states.get(command.buyerCommand().aggregateId());
            MatchingState sellerState = states.get(command.sellerCommand().aggregateId());
            if (!buyerState.canMatch(command.buyerMatchedQuantity())
                    || !sellerState.canMatch(command.sellerMatchedQuantity())) {
                return TradeApplicationBatchAppendResult.notBatchable(
                        TradeApplicationBatchNotBatchableReason.INVALID_HEAD_STATE);
            }
            preparedBatch.add(new TradeApplicationHotPathBatchAppend(
                    command.buyerCommand().aggregateId(),
                    command.buyerMatchedQuantity(),
                    buyerState,
                    command.sellerCommand().aggregateId(),
                    command.sellerMatchedQuantity(),
                    sellerState,
                    command.tradeApplication(),
                    command.integrationEvent(),
                    commandWithPayload.integrationPayload()));
        }
        tradeApplyMetrics.record("batch_prepare_append", prepareStartedNanos);

        long appendStartedNanos = System.nanoTime();
        TradeApplicationHotPathBatchOutcome outcome = insertTradeApplicationsMatchingStatesAndOutboxes(preparedBatch);
        tradeApplyMetrics.record("batch_append", appendStartedNanos);
        if (outcome.existingTradeApplications() > 0) {
            return TradeApplicationBatchAppendResult.notBatchable(
                    TradeApplicationBatchNotBatchableReason.EXISTING_TRADE_APPLICATION);
        }
        if (outcome.insertedTradeApplications() != preparedBatch.size()) {
            throw new IllegalStateException("Expected " + preparedBatch.size()
                    + " order trade applications to be inserted, actual="
                    + outcome.insertedTradeApplications());
        }
        if (outcome.updatedMatchingStates() != preparedBatch.size() * 2) {
            throw new IllegalStateException("Expected " + preparedBatch.size() * 2
                    + " order matching states to be updated, actual="
                    + outcome.updatedMatchingStates());
        }
        int expectedOutboxes = (int) preparedBatch.stream()
                .filter(prepared -> prepared.integrationEvent() != null)
                .count();
        if (outcome.insertedOutboxes() != expectedOutboxes) {
            throw new IllegalStateException("Expected " + expectedOutboxes
                    + " order outbox rows to be inserted, actual="
                    + outcome.insertedOutboxes());
        }
        return TradeApplicationBatchAppendResult.applied(preparedBatch.size());
    }

    private void validateDistinctTradeOrders(
            OrderEventAppendCommand buyerCommand,
            OrderEventAppendCommand sellerCommand) {
        if (buyerCommand.aggregateId().equals(sellerCommand.aggregateId())) {
            throw new IllegalArgumentException("Buyer and seller order must be different: " + buyerCommand.aggregateId());
        }
    }

    private Map<UUID, StreamHead> lockHeadsInStableOrder(UUID first, UUID second) {
        return lockHeadsInStableOrder(stableOrder(first, second));
    }

    private Map<UUID, StreamHead> lockHeadsInStableOrder(Collection<UUID> aggregateIds) {
        Map<UUID, StreamHead> heads = new HashMap<>();
        consumerJdbc.query("""
                SELECT aggregate_id, current_version, last_hash, user_id, remaining_amount, status
                FROM order_service.order_stream_heads
                WHERE aggregate_id IN (:aggregateIds)
                ORDER BY aggregate_id
                FOR UPDATE
                """, Map.of("aggregateIds", aggregateIds), rs -> {
            heads.put(
                    rs.getObject("aggregate_id", UUID.class),
                    new StreamHead(
                            rs.getLong("current_version"),
                            rs.getString("last_hash"),
                            rs.getObject("user_id", UUID.class),
                            (Integer) rs.getObject("remaining_amount"),
                            rs.getString("status")));
        });
        return heads;
    }

    private Map<UUID, MatchingState> lockMatchingStatesInStableOrder(UUID first, UUID second) {
        return lockMatchingStatesInStableOrder(stableOrder(first, second));
    }

    private Map<UUID, MatchingState> lockMatchingStatesInStableOrder(Collection<UUID> orderIds) {
        Map<UUID, MatchingState> states = new HashMap<>();
        consumerJdbc.query("""
                SELECT order_id, user_id, remaining_amount, matched_amount, status
                FROM order_service.order_matching_state
                WHERE order_id IN (:orderIds)
                ORDER BY order_id
                FOR UPDATE
                """, Map.of("orderIds", orderIds), rs -> {
            states.put(
                    rs.getObject("order_id", UUID.class),
                    new MatchingState(
                            rs.getObject("user_id", UUID.class),
                            rs.getInt("remaining_amount"),
                            rs.getInt("matched_amount"),
                            rs.getString("status")));
        });
        return states;
    }

    private MatchingState lockMatchingState(
            NamedParameterJdbcTemplate jdbc,
            UUID orderId) {
        List<MatchingState> states = jdbc.query("""
                SELECT user_id, remaining_amount, matched_amount, status
                FROM order_service.order_matching_state
                WHERE order_id = :orderId
                FOR UPDATE
                """, new MapSqlParameterSource("orderId", orderId),
                (rs, rowNum) -> new MatchingState(
                        rs.getObject("user_id", UUID.class),
                        rs.getInt("remaining_amount"),
                        rs.getInt("matched_amount"),
                        rs.getString("status")));
        return states.isEmpty() ? null : states.get(0);
    }

    private List<UUID> stableOrder(UUID first, UUID second) {
        return first.compareTo(second) <= 0 ? List.of(first, second) : List.of(second, first);
    }

    private boolean existingTradeApplicationMatches(OrderTradeApplication tradeApplication) {
        Integer count = consumerJdbc.queryForObject("""
                SELECT COUNT(*)
                FROM order_service.order_trade_applications
                WHERE trade_id = :tradeId
                  AND buyer_order_id = :buyerOrderId
                  AND seller_order_id = :sellerOrderId
                  AND price = :price
                  AND quantity = :quantity
                  AND applied_at = :appliedAt
                """, new MapSqlParameterSource()
                .addValue("tradeId", tradeApplication.tradeId())
                .addValue("buyerOrderId", tradeApplication.buyerOrderId())
                .addValue("sellerOrderId", tradeApplication.sellerOrderId())
                .addValue("price", tradeApplication.price())
                .addValue("quantity", tradeApplication.quantity())
                .addValue("appliedAt", tradeApplication.appliedAt()), Integer.class);
        return count != null && count == 1;
    }

    private boolean hasExistingTradeApplications(List<String> tradeIds) {
        Integer count = consumerJdbc.queryForObject("""
                SELECT COUNT(*)
                FROM order_service.order_trade_applications
                WHERE trade_id IN (:tradeIds)
                """, Map.of("tradeIds", tradeIds), Integer.class);
        return count != null && count > 0;
    }

    private TradeApplicationHotPathOutcome insertTradeApplicationMatchingStateAndOutbox(
            UUID buyerOrderId,
            int buyerMatchedQuantity,
            MatchingState buyerState,
            UUID sellerOrderId,
            int sellerMatchedQuantity,
            MatchingState sellerState,
            OrderTradeApplication tradeApplication,
            OrderIntegrationEvent integrationEvent,
            String integrationPayload) {
        final int[] insertedTradeApplications = {0};
        final int[] updatedMatchingStates = {0};
        final int[] insertedOutboxes = {0};
        consumerJdbc.query("""
                WITH trade_application AS (
                    INSERT INTO order_service.order_trade_applications
                        (trade_id, buyer_order_id, seller_order_id, price, quantity, applied_at)
                    VALUES
                        (:tradeId, :tradeBuyerOrderId, :tradeSellerOrderId,
                         :tradePrice, :tradeQuantity, :tradeAppliedAt)
                    ON CONFLICT (trade_id) DO NOTHING
                    RETURNING trade_id
                ),
                input(order_id, quantity, previous_remaining_amount, remaining_amount, matched_amount,
                      order_status, last_trade_id) AS (
                    VALUES
                        (:buyerOrderId, :buyerQuantity, :buyerPreviousRemainingAmount,
                         :buyerRemainingAmount, :buyerMatchedAmount, :buyerStatus, :tradeId),
                        (:sellerOrderId, :sellerQuantity, :sellerPreviousRemainingAmount,
                         :sellerRemainingAmount, :sellerMatchedAmount, :sellerStatus, :tradeId)
                ),
                updated_matching_states AS (
                    UPDATE order_service.order_matching_state state
                    SET remaining_amount = input.remaining_amount,
                        matched_amount = input.matched_amount,
                        status = input.order_status,
                        last_trade_id = input.last_trade_id,
                        updated_at = CURRENT_TIMESTAMP
                    FROM input
                    WHERE EXISTS (SELECT 1 FROM trade_application)
                      AND state.order_id = input.order_id
                      AND state.remaining_amount = input.previous_remaining_amount
                      AND state.status IN ('OPEN', 'PARTIALLY_MATCHED')
                      AND state.remaining_amount >= input.quantity
                    RETURNING 1
                ),
                inserted_outbox AS (
                    INSERT INTO order_service.order_event_outbox
                        (event_id, aggregate_id, exchange_name, routing_key, message_type, payload,
                         status, attempt_count, next_retry_at, created_at, updated_at)
                    SELECT :outboxEventId, :outboxAggregateId, :outboxExchange, :outboxRoutingKey,
                           :outboxMessageType, :outboxPayload,
                           'PENDING', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    WHERE EXISTS (SELECT 1 FROM trade_application)
                      AND :outboxEventId IS NOT NULL
                    RETURNING 1
                )
                SELECT
                    (SELECT COUNT(*) FROM trade_application) AS inserted_trade_applications,
                    (SELECT COUNT(*) FROM updated_matching_states) AS updated_matching_states,
                    (SELECT COUNT(*) FROM inserted_outbox) AS inserted_outboxes
                """, tradeApplicationHotPathParams(
                buyerOrderId,
                buyerMatchedQuantity,
                buyerState,
                sellerOrderId,
                sellerMatchedQuantity,
                sellerState,
                tradeApplication,
                integrationEvent,
                integrationPayload), rs -> {
            insertedTradeApplications[0] = rs.getInt("inserted_trade_applications");
            updatedMatchingStates[0] = rs.getInt("updated_matching_states");
            insertedOutboxes[0] = rs.getInt("inserted_outboxes");
        });
        return new TradeApplicationHotPathOutcome(
                insertedTradeApplications[0],
                updatedMatchingStates[0],
                insertedOutboxes[0]);
    }

    private void insertTradeApplicationRows(List<TradeApplicationHotPathBatchAppend> preparedBatch) {
        int[] counts = consumerJdbc.batchUpdate("""
                INSERT INTO order_service.order_trade_applications
                    (trade_id, buyer_order_id, seller_order_id, price, quantity, applied_at)
                VALUES
                    (:tradeId, :buyerOrderId, :sellerOrderId, :price, :quantity, :appliedAt)
                ON CONFLICT (trade_id) DO NOTHING
                """, preparedBatch.stream()
                .map(prepared -> tradeApplicationParams(prepared.tradeApplication()))
                .toArray(MapSqlParameterSource[]::new));
        assertBatchCount("trade application", preparedBatch.size(), counts);
    }

    private void updateMatchingStates(List<TradeApplicationHotPathBatchAppend> preparedBatch) {
        List<MapSqlParameterSource> params = new ArrayList<>(preparedBatch.size() * 2);
        for (TradeApplicationHotPathBatchAppend prepared : preparedBatch) {
            params.add(matchingStateUpdateParams(
                    prepared.buyerOrderId(),
                    prepared.buyerMatchedQuantity(),
                    prepared.buyerState(),
                    prepared.tradeApplication().tradeId()));
            params.add(matchingStateUpdateParams(
                    prepared.sellerOrderId(),
                    prepared.sellerMatchedQuantity(),
                    prepared.sellerState(),
                    prepared.tradeApplication().tradeId()));
        }
        int[] counts = consumerJdbc.batchUpdate("""
                UPDATE order_service.order_matching_state
                SET remaining_amount = :remainingAmount,
                    matched_amount = :matchedAmount,
                    status = :orderStatus,
                    last_trade_id = :tradeId,
                    updated_at = CURRENT_TIMESTAMP
                WHERE order_id = :orderId
                  AND remaining_amount = :previousRemainingAmount
                  AND status IN ('OPEN', 'PARTIALLY_MATCHED')
                  AND remaining_amount >= :quantity
                """, params.toArray(MapSqlParameterSource[]::new));
        assertBatchCount("matching state", params.size(), counts);
    }

    private void insertHotPathOutboxes(List<TradeApplicationHotPathBatchAppend> preparedBatch) {
        MapSqlParameterSource[] params = preparedBatch.stream()
                .filter(prepared -> prepared.integrationEvent() != null)
                .map(this::hotPathOutboxParams)
                .toArray(MapSqlParameterSource[]::new);
        if (params.length == 0) {
            return;
        }
        int[] counts = consumerJdbc.batchUpdate("""
                INSERT INTO order_service.order_event_outbox
                    (event_id, aggregate_id, exchange_name, routing_key, message_type, payload,
                     status, attempt_count, next_retry_at, created_at, updated_at)
                VALUES
                    (:eventId, :aggregateId, :exchange, :routingKey, :messageType, :payload,
                     'PENDING', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, params);
        assertBatchCount("outbox", params.length, counts);
    }

    private TradeApplicationHotPathBatchOutcome insertTradeApplicationsMatchingStatesAndOutboxes(
            List<TradeApplicationHotPathBatchAppend> preparedBatch) {
        if (preparedBatch.isEmpty()) {
            return new TradeApplicationHotPathBatchOutcome(0, 0, 0, 0);
        }
        return consumerJdbc.getJdbcTemplate().execute((ConnectionCallback<TradeApplicationHotPathBatchOutcome>) connection -> {
            Array tradeIds = null;
            Array tradeBuyerOrderIds = null;
            Array tradeSellerOrderIds = null;
            Array tradePrices = null;
            Array tradeQuantities = null;
            Array tradeAppliedAts = null;
            Array buyerOrderIds = null;
            Array buyerQuantities = null;
            Array buyerPreviousRemainingAmounts = null;
            Array buyerRemainingAmounts = null;
            Array buyerMatchedAmounts = null;
            Array buyerStatuses = null;
            Array sellerOrderIds = null;
            Array sellerQuantities = null;
            Array sellerPreviousRemainingAmounts = null;
            Array sellerRemainingAmounts = null;
            Array sellerMatchedAmounts = null;
            Array sellerStatuses = null;
            Array outboxEventIds = null;
            Array outboxAggregateIds = null;
            Array outboxExchanges = null;
            Array outboxRoutingKeys = null;
            Array outboxMessageTypes = null;
            Array outboxPayloads = null;
            try (PreparedStatement statement = connection.prepareStatement(
                    INSERT_TRADE_APPLICATIONS_MATCHING_STATES_AND_OUTBOXES_SQL)) {
                TradeApplicationBatchArrays arrays = tradeApplicationBatchArrays(preparedBatch);
                tradeIds = connection.createArrayOf("varchar", arrays.tradeIds());
                tradeBuyerOrderIds = connection.createArrayOf("uuid", arrays.tradeBuyerOrderIds());
                tradeSellerOrderIds = connection.createArrayOf("uuid", arrays.tradeSellerOrderIds());
                tradePrices = connection.createArrayOf("integer", arrays.tradePrices());
                tradeQuantities = connection.createArrayOf("integer", arrays.tradeQuantities());
                tradeAppliedAts = connection.createArrayOf("timestamp", arrays.tradeAppliedAts());
                buyerOrderIds = connection.createArrayOf("uuid", arrays.buyerOrderIds());
                buyerQuantities = connection.createArrayOf("integer", arrays.buyerQuantities());
                buyerPreviousRemainingAmounts = connection.createArrayOf("integer", arrays.buyerPreviousRemainingAmounts());
                buyerRemainingAmounts = connection.createArrayOf("integer", arrays.buyerRemainingAmounts());
                buyerMatchedAmounts = connection.createArrayOf("integer", arrays.buyerMatchedAmounts());
                buyerStatuses = connection.createArrayOf("varchar", arrays.buyerStatuses());
                sellerOrderIds = connection.createArrayOf("uuid", arrays.sellerOrderIds());
                sellerQuantities = connection.createArrayOf("integer", arrays.sellerQuantities());
                sellerPreviousRemainingAmounts = connection.createArrayOf("integer", arrays.sellerPreviousRemainingAmounts());
                sellerRemainingAmounts = connection.createArrayOf("integer", arrays.sellerRemainingAmounts());
                sellerMatchedAmounts = connection.createArrayOf("integer", arrays.sellerMatchedAmounts());
                sellerStatuses = connection.createArrayOf("varchar", arrays.sellerStatuses());
                outboxEventIds = connection.createArrayOf("uuid", arrays.outboxEventIds());
                outboxAggregateIds = connection.createArrayOf("uuid", arrays.outboxAggregateIds());
                outboxExchanges = connection.createArrayOf("varchar", arrays.outboxExchanges());
                outboxRoutingKeys = connection.createArrayOf("varchar", arrays.outboxRoutingKeys());
                outboxMessageTypes = connection.createArrayOf("varchar", arrays.outboxMessageTypes());
                outboxPayloads = connection.createArrayOf("text", arrays.outboxPayloads());

                statement.setArray(1, tradeIds);
                statement.setArray(2, tradeBuyerOrderIds);
                statement.setArray(3, tradeSellerOrderIds);
                statement.setArray(4, tradePrices);
                statement.setArray(5, tradeQuantities);
                statement.setArray(6, tradeAppliedAts);
                statement.setArray(7, buyerOrderIds);
                statement.setArray(8, buyerQuantities);
                statement.setArray(9, buyerPreviousRemainingAmounts);
                statement.setArray(10, buyerRemainingAmounts);
                statement.setArray(11, buyerMatchedAmounts);
                statement.setArray(12, buyerStatuses);
                statement.setArray(13, sellerOrderIds);
                statement.setArray(14, sellerQuantities);
                statement.setArray(15, sellerPreviousRemainingAmounts);
                statement.setArray(16, sellerRemainingAmounts);
                statement.setArray(17, sellerMatchedAmounts);
                statement.setArray(18, sellerStatuses);
                statement.setArray(19, outboxEventIds);
                statement.setArray(20, outboxAggregateIds);
                statement.setArray(21, outboxExchanges);
                statement.setArray(22, outboxRoutingKeys);
                statement.setArray(23, outboxMessageTypes);
                statement.setArray(24, outboxPayloads);

                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Order trade application batch did not return an outcome");
                    }
                    return new TradeApplicationHotPathBatchOutcome(
                            rs.getInt("existing_trade_applications"),
                            rs.getInt("inserted_trade_applications"),
                            rs.getInt("updated_matching_states"),
                            rs.getInt("inserted_outboxes"));
                }
            } finally {
                freeQuietly(tradeIds);
                freeQuietly(tradeBuyerOrderIds);
                freeQuietly(tradeSellerOrderIds);
                freeQuietly(tradePrices);
                freeQuietly(tradeQuantities);
                freeQuietly(tradeAppliedAts);
                freeQuietly(buyerOrderIds);
                freeQuietly(buyerQuantities);
                freeQuietly(buyerPreviousRemainingAmounts);
                freeQuietly(buyerRemainingAmounts);
                freeQuietly(buyerMatchedAmounts);
                freeQuietly(buyerStatuses);
                freeQuietly(sellerOrderIds);
                freeQuietly(sellerQuantities);
                freeQuietly(sellerPreviousRemainingAmounts);
                freeQuietly(sellerRemainingAmounts);
                freeQuietly(sellerMatchedAmounts);
                freeQuietly(sellerStatuses);
                freeQuietly(outboxEventIds);
                freeQuietly(outboxAggregateIds);
                freeQuietly(outboxExchanges);
                freeQuietly(outboxRoutingKeys);
                freeQuietly(outboxMessageTypes);
                freeQuietly(outboxPayloads);
            }
        });
    }

    private void assertBatchCount(String label, int expected, int[] counts) {
        if (counts.length != expected) {
            throw new IllegalStateException("Expected " + expected + " " + label
                    + " batch results, actual=" + counts.length);
        }
        for (int count : counts) {
            if (count != 1) {
                throw new IllegalStateException("Expected every " + label
                        + " batch row to affect one row, actual=" + count);
            }
        }
    }

    private OrderEventAppendResult appendInTransactionWithLockedHead(
            OrderEventAppendCommand command,
            NamedParameterJdbcTemplate jdbc,
            StreamHead head) {
        String payloadCanonical = serialize(command.payload());
        String metadataCanonical = serialize(command.metadata());

        if (head.currentVersion() != command.expectedVersion()) {
            ExistingEvent existing = findByEventId(jdbc, command.eventId());
            if (existing != null) {
                return existingAppendResult(command, existing, payloadCanonical, metadataCanonical);
            }
            throw new OrderEventVersionConflictException(
                    command.aggregateId(), command.expectedVersion(), head.currentVersion());
        }

        long nextVersion = head.currentVersion() + 1;
        String hash = computeHash(
                command,
                nextVersion,
                payloadCanonical,
                metadataCanonical,
                head.lastHash()
        );
        long globalPosition;
        try {
            globalPosition = insertEvent(
                    jdbc,
                    command,
                    nextVersion,
                    payloadCanonical,
                    metadataCanonical,
                    head.lastHash(),
                    hash
            );
        } catch (DuplicateKeyException e) {
            ExistingEvent existing = findByEventId(jdbc, command.eventId());
            if (existing != null) {
                return existingAppendResult(command, existing, payloadCanonical, metadataCanonical);
            }
            throw e;
        }
        updateHead(jdbc, command, head, nextVersion, hash);
        insertOutboxIfPresent(jdbc, command, payloadCanonical);

        return new OrderEventAppendResult(
                command.aggregateId(),
                command.eventId(),
                nextVersion,
                globalPosition,
                hash,
                false
        );
    }

    private OrderEventAppendResult existingAppendResult(
            OrderEventAppendCommand command,
            ExistingEvent existing,
            String payloadCanonical,
            String metadataCanonical) {
        if (!existing.aggregateId().equals(command.aggregateId())
                || !existing.eventType().equals(command.eventType())
                || !existing.payloadCanonical().equals(payloadCanonical)
                || !existing.metadataCanonical().equals(metadataCanonical)
                || existing.schemaVersion() != command.schemaVersion()
                || !existing.occurredAt().equals(command.occurredAt())) {
            throw new OrderEventIdentityConflictException(command.eventId());
        }
        return new OrderEventAppendResult(
                existing.aggregateId(),
                command.eventId(),
                existing.aggregateVersion(),
                existing.globalPosition(),
                existing.hash(),
                true
        );
    }

    private void createHeadIfAbsent(NamedParameterJdbcTemplate jdbc, UUID aggregateId) {
        jdbc.update("""
                INSERT INTO order_service.order_stream_heads
                    (aggregate_id, current_version, last_hash, updated_at)
                VALUES (:aggregateId, 0, :genesisHash, CURRENT_TIMESTAMP)
                ON CONFLICT (aggregate_id) DO NOTHING
                """, new MapSqlParameterSource()
                .addValue("aggregateId", aggregateId)
                .addValue("genesisHash", GENESIS_HASH));
    }

    private StreamHead lockHead(NamedParameterJdbcTemplate jdbc, UUID aggregateId) {
        List<StreamHead> rows = jdbc.query("""
                SELECT current_version, last_hash, user_id, remaining_amount, status
                FROM order_service.order_stream_heads
                WHERE aggregate_id = :aggregateId
                FOR UPDATE
                """, Map.of("aggregateId", aggregateId),
                (rs, rowNum) -> new StreamHead(
                        rs.getLong("current_version"),
                        rs.getString("last_hash"),
                        rs.getObject("user_id", UUID.class),
                        (Integer) rs.getObject("remaining_amount"),
                        rs.getString("status")));
        if (rows.size() != 1) {
            throw new IllegalStateException("Order stream head not found after creation: " + aggregateId);
        }
        return rows.get(0);
    }

    private ExistingEvent findByEventId(NamedParameterJdbcTemplate jdbc, UUID eventId) {
        List<ExistingEvent> rows = jdbc.query("""
                SELECT aggregate_id, aggregate_version, event_type,
                       payload_canonical, metadata_canonical, schema_version,
                       occurred_at, global_position, hash
                FROM order_service.order_event_store
                WHERE event_id = :eventId
                """, Map.of("eventId", eventId),
                (rs, rowNum) -> new ExistingEvent(
                        rs.getObject("aggregate_id", UUID.class),
                        rs.getLong("aggregate_version"),
                        rs.getString("event_type"),
                        rs.getString("payload_canonical"),
                        rs.getString("metadata_canonical"),
                        rs.getInt("schema_version"),
                        rs.getObject("occurred_at", LocalDateTime.class),
                        rs.getLong("global_position"),
                        rs.getString("hash")
                ));
        return rows.isEmpty() ? null : rows.get(0);
    }

    private MapSqlParameterSource tradeApplicationParams(OrderTradeApplication tradeApplication) {
        return new MapSqlParameterSource()
                .addValue("tradeId", tradeApplication.tradeId())
                .addValue("buyerOrderId", tradeApplication.buyerOrderId())
                .addValue("sellerOrderId", tradeApplication.sellerOrderId())
                .addValue("price", tradeApplication.price())
                .addValue("quantity", tradeApplication.quantity())
                .addValue("appliedAt", tradeApplication.appliedAt());
    }

    private MapSqlParameterSource tradeApplicationHotPathParams(
            UUID buyerOrderId,
            int buyerMatchedQuantity,
            MatchingState buyerState,
            UUID sellerOrderId,
            int sellerMatchedQuantity,
            MatchingState sellerState,
            OrderTradeApplication tradeApplication,
            OrderIntegrationEvent integrationEvent,
            String integrationPayload) {
        MapSqlParameterSource params = tradeApplicationAppendParams(
                buyerOrderId,
                buyerMatchedQuantity,
                buyerState,
                sellerOrderId,
                sellerMatchedQuantity,
                sellerState,
                tradeApplication);
        if (integrationEvent == null) {
            return params
                    .addValue("outboxEventId", null)
                    .addValue("outboxAggregateId", null)
                    .addValue("outboxExchange", null)
                    .addValue("outboxRoutingKey", null)
                    .addValue("outboxMessageType", null)
                    .addValue("outboxPayload", null);
        }
        return params
                .addValue("outboxEventId", UUID.nameUUIDFromBytes(
                        ("ORDER_TRADE_APPLIED:" + tradeApplication.tradeId()).getBytes(StandardCharsets.UTF_8)))
                .addValue("outboxAggregateId", buyerOrderId)
                .addValue("outboxExchange", integrationEvent.exchange())
                .addValue("outboxRoutingKey", integrationEvent.routingKey())
                .addValue("outboxMessageType", integrationEvent.payload().getClass().getName())
                .addValue("outboxPayload", integrationPayload);
    }

    private MapSqlParameterSource tradeApplicationAppendParams(
            UUID buyerOrderId,
            int buyerMatchedQuantity,
            MatchingState buyerState,
            UUID sellerOrderId,
            int sellerMatchedQuantity,
            MatchingState sellerState,
            OrderTradeApplication tradeApplication) {
        MatchingState buyerNext = buyerState.apply(buyerMatchedQuantity);
        MatchingState sellerNext = sellerState.apply(sellerMatchedQuantity);
        return new MapSqlParameterSource()
                .addValue("tradeId", tradeApplication.tradeId())
                .addValue("tradeBuyerOrderId", tradeApplication.buyerOrderId())
                .addValue("tradeSellerOrderId", tradeApplication.sellerOrderId())
                .addValue("tradePrice", tradeApplication.price())
                .addValue("tradeQuantity", tradeApplication.quantity())
                .addValue("tradeAppliedAt", tradeApplication.appliedAt())
                .addValue("buyerOrderId", buyerOrderId)
                .addValue("buyerQuantity", buyerMatchedQuantity)
                .addValue("buyerPreviousRemainingAmount", buyerState.remainingAmount())
                .addValue("buyerRemainingAmount", buyerNext.remainingAmount())
                .addValue("buyerMatchedAmount", buyerNext.matchedAmount())
                .addValue("buyerStatus", buyerNext.status())
                .addValue("sellerOrderId", sellerOrderId)
                .addValue("sellerQuantity", sellerMatchedQuantity)
                .addValue("sellerPreviousRemainingAmount", sellerState.remainingAmount())
                .addValue("sellerRemainingAmount", sellerNext.remainingAmount())
                .addValue("sellerMatchedAmount", sellerNext.matchedAmount())
                .addValue("sellerStatus", sellerNext.status());
    }

    private MapSqlParameterSource matchingStateUpdateParams(
            UUID orderId,
            int matchedQuantity,
            MatchingState state,
            String tradeId) {
        MatchingState next = state.apply(matchedQuantity);
        return new MapSqlParameterSource()
                .addValue("orderId", orderId)
                .addValue("quantity", matchedQuantity)
                .addValue("previousRemainingAmount", state.remainingAmount())
                .addValue("remainingAmount", next.remainingAmount())
                .addValue("matchedAmount", next.matchedAmount())
                .addValue("orderStatus", next.status())
                .addValue("tradeId", tradeId);
    }

    private MapSqlParameterSource hotPathOutboxParams(TradeApplicationHotPathBatchAppend prepared) {
        OrderIntegrationEvent integrationEvent = prepared.integrationEvent();
        return new MapSqlParameterSource()
                .addValue("eventId", UUID.nameUUIDFromBytes(
                        ("ORDER_TRADE_APPLIED:" + prepared.tradeApplication().tradeId())
                                .getBytes(StandardCharsets.UTF_8)))
                .addValue("aggregateId", prepared.buyerOrderId())
                .addValue("exchange", integrationEvent.exchange())
                .addValue("routingKey", integrationEvent.routingKey())
                .addValue("messageType", integrationEvent.payload().getClass().getName())
                .addValue("payload", prepared.integrationPayload());
    }

    private TradeApplicationBatchArrays tradeApplicationBatchArrays(
            List<TradeApplicationHotPathBatchAppend> preparedBatch) {
        int size = preparedBatch.size();
        String[] tradeIds = new String[size];
        UUID[] tradeBuyerOrderIds = new UUID[size];
        UUID[] tradeSellerOrderIds = new UUID[size];
        Integer[] tradePrices = new Integer[size];
        Integer[] tradeQuantities = new Integer[size];
        Timestamp[] tradeAppliedAts = new Timestamp[size];
        UUID[] buyerOrderIds = new UUID[size];
        Integer[] buyerQuantities = new Integer[size];
        Integer[] buyerPreviousRemainingAmounts = new Integer[size];
        Integer[] buyerRemainingAmounts = new Integer[size];
        Integer[] buyerMatchedAmounts = new Integer[size];
        String[] buyerStatuses = new String[size];
        UUID[] sellerOrderIds = new UUID[size];
        Integer[] sellerQuantities = new Integer[size];
        Integer[] sellerPreviousRemainingAmounts = new Integer[size];
        Integer[] sellerRemainingAmounts = new Integer[size];
        Integer[] sellerMatchedAmounts = new Integer[size];
        String[] sellerStatuses = new String[size];
        UUID[] outboxEventIds = new UUID[size];
        UUID[] outboxAggregateIds = new UUID[size];
        String[] outboxExchanges = new String[size];
        String[] outboxRoutingKeys = new String[size];
        String[] outboxMessageTypes = new String[size];
        String[] outboxPayloads = new String[size];

        for (int i = 0; i < size; i++) {
            TradeApplicationHotPathBatchAppend prepared = preparedBatch.get(i);
            MatchingState buyerNext = prepared.buyerState().apply(prepared.buyerMatchedQuantity());
            MatchingState sellerNext = prepared.sellerState().apply(prepared.sellerMatchedQuantity());
            OrderTradeApplication tradeApplication = prepared.tradeApplication();

            tradeIds[i] = tradeApplication.tradeId();
            tradeBuyerOrderIds[i] = tradeApplication.buyerOrderId();
            tradeSellerOrderIds[i] = tradeApplication.sellerOrderId();
            tradePrices[i] = tradeApplication.price();
            tradeQuantities[i] = tradeApplication.quantity();
            tradeAppliedAts[i] = Timestamp.valueOf(tradeApplication.appliedAt());
            buyerOrderIds[i] = prepared.buyerOrderId();
            buyerQuantities[i] = prepared.buyerMatchedQuantity();
            buyerPreviousRemainingAmounts[i] = prepared.buyerState().remainingAmount();
            buyerRemainingAmounts[i] = buyerNext.remainingAmount();
            buyerMatchedAmounts[i] = buyerNext.matchedAmount();
            buyerStatuses[i] = buyerNext.status();
            sellerOrderIds[i] = prepared.sellerOrderId();
            sellerQuantities[i] = prepared.sellerMatchedQuantity();
            sellerPreviousRemainingAmounts[i] = prepared.sellerState().remainingAmount();
            sellerRemainingAmounts[i] = sellerNext.remainingAmount();
            sellerMatchedAmounts[i] = sellerNext.matchedAmount();
            sellerStatuses[i] = sellerNext.status();

            OrderIntegrationEvent integrationEvent = prepared.integrationEvent();
            if (integrationEvent != null) {
                outboxEventIds[i] = UUID.nameUUIDFromBytes(
                        ("ORDER_TRADE_APPLIED:" + tradeApplication.tradeId())
                                .getBytes(StandardCharsets.UTF_8));
                outboxAggregateIds[i] = prepared.buyerOrderId();
                outboxExchanges[i] = integrationEvent.exchange();
                outboxRoutingKeys[i] = integrationEvent.routingKey();
                outboxMessageTypes[i] = integrationEvent.payload().getClass().getName();
                outboxPayloads[i] = prepared.integrationPayload();
            }
        }
        return new TradeApplicationBatchArrays(
                tradeIds,
                tradeBuyerOrderIds,
                tradeSellerOrderIds,
                tradePrices,
                tradeQuantities,
                tradeAppliedAts,
                buyerOrderIds,
                buyerQuantities,
                buyerPreviousRemainingAmounts,
                buyerRemainingAmounts,
                buyerMatchedAmounts,
                buyerStatuses,
                sellerOrderIds,
                sellerQuantities,
                sellerPreviousRemainingAmounts,
                sellerRemainingAmounts,
                sellerMatchedAmounts,
                sellerStatuses,
                outboxEventIds,
                outboxAggregateIds,
                outboxExchanges,
                outboxRoutingKeys,
                outboxMessageTypes,
                outboxPayloads);
    }

    private void freeQuietly(Array array) {
        if (array == null) {
            return;
        }
        try {
            array.free();
        } catch (Exception ignored) {
        }
    }

    private void addHotPathBatchParams(
            MapSqlParameterSource params,
            TradeApplicationHotPathBatchAppend prepared,
            int index) {
        MatchingState buyerNext = prepared.buyerState().apply(prepared.buyerMatchedQuantity());
        MatchingState sellerNext = prepared.sellerState().apply(prepared.sellerMatchedQuantity());
        OrderTradeApplication tradeApplication = prepared.tradeApplication();
        params.addValue("tradeId" + index, tradeApplication.tradeId())
                .addValue("tradeBuyerOrderId" + index, tradeApplication.buyerOrderId())
                .addValue("tradeSellerOrderId" + index, tradeApplication.sellerOrderId())
                .addValue("tradePrice" + index, tradeApplication.price())
                .addValue("tradeQuantity" + index, tradeApplication.quantity())
                .addValue("tradeAppliedAt" + index, tradeApplication.appliedAt())
                .addValue("buyerOrderId" + index, prepared.buyerOrderId())
                .addValue("buyerQuantity" + index, prepared.buyerMatchedQuantity())
                .addValue("buyerPreviousRemainingAmount" + index, prepared.buyerState().remainingAmount())
                .addValue("buyerRemainingAmount" + index, buyerNext.remainingAmount())
                .addValue("buyerMatchedAmount" + index, buyerNext.matchedAmount())
                .addValue("buyerStatus" + index, buyerNext.status())
                .addValue("sellerOrderId" + index, prepared.sellerOrderId())
                .addValue("sellerQuantity" + index, prepared.sellerMatchedQuantity())
                .addValue("sellerPreviousRemainingAmount" + index, prepared.sellerState().remainingAmount())
                .addValue("sellerRemainingAmount" + index, sellerNext.remainingAmount())
                .addValue("sellerMatchedAmount" + index, sellerNext.matchedAmount())
                .addValue("sellerStatus" + index, sellerNext.status());
        if (prepared.integrationEvent() == null) {
            params.addValue("outboxEventId" + index, null)
                    .addValue("outboxAggregateId" + index, null)
                    .addValue("outboxExchange" + index, null)
                    .addValue("outboxRoutingKey" + index, null)
                    .addValue("outboxMessageType" + index, null)
                    .addValue("outboxPayload" + index, null);
            return;
        }
        OrderIntegrationEvent integrationEvent = prepared.integrationEvent();
        params.addValue("outboxEventId" + index, UUID.nameUUIDFromBytes(
                        ("ORDER_TRADE_APPLIED:" + tradeApplication.tradeId())
                                .getBytes(StandardCharsets.UTF_8)))
                .addValue("outboxAggregateId" + index, prepared.buyerOrderId())
                .addValue("outboxExchange" + index, integrationEvent.exchange())
                .addValue("outboxRoutingKey" + index, integrationEvent.routingKey())
                .addValue("outboxMessageType" + index, integrationEvent.payload().getClass().getName())
                .addValue("outboxPayload" + index, prepared.integrationPayload());
    }

    private long insertEvent(
            NamedParameterJdbcTemplate jdbc,
            OrderEventAppendCommand command,
            long aggregateVersion,
            String payloadCanonical,
            String metadataCanonical,
            String prevHash,
            String hash) {
        Long position = jdbc.queryForObject("""
                INSERT INTO order_service.order_event_store
                    (event_id, aggregate_id, aggregate_type, aggregate_version,
                     event_type, payload_canonical, metadata_canonical,
                     schema_version, occurred_at, prev_hash, hash)
                VALUES
                    (:eventId, :aggregateId, :aggregateType, :aggregateVersion,
                     :eventType, :payload,
                     :metadata,
                     :schemaVersion, :occurredAt, :prevHash, :hash)
                RETURNING global_position
                """, new MapSqlParameterSource()
                .addValue("eventId", command.eventId())
                .addValue("aggregateId", command.aggregateId())
                .addValue("aggregateType", AGGREGATE_TYPE)
                .addValue("aggregateVersion", aggregateVersion)
                .addValue("eventType", command.eventType())
                .addValue("payload", payloadCanonical)
                .addValue("metadata", metadataCanonical)
                .addValue("schemaVersion", command.schemaVersion())
                .addValue("occurredAt", command.occurredAt())
                .addValue("prevHash", prevHash)
                .addValue("hash", hash), Long.class);
        if (position == null) {
            throw new IllegalStateException("Event Store insert did not return global position");
        }
        return position;
    }

    private void updateHead(
            NamedParameterJdbcTemplate jdbc,
            OrderEventAppendCommand command,
            StreamHead head,
            long newVersion,
            String hash) {
        CommandState nextState = nextCommandState(head, command);
        int updated = jdbc.update("""
                UPDATE order_service.order_stream_heads
                SET current_version = :newVersion,
                    last_event_id = :eventId,
                    last_hash = :hash,
                    user_id = :userId,
                    remaining_amount = :remainingAmount,
                    status = :orderStatus,
                    updated_at = CURRENT_TIMESTAMP
                WHERE aggregate_id = :aggregateId
                  AND current_version = :expectedVersion
                """, new MapSqlParameterSource()
                .addValue("newVersion", newVersion)
                .addValue("eventId", command.eventId())
                .addValue("hash", hash)
                .addValue("userId", nextState.userId())
                .addValue("remainingAmount", nextState.remainingAmount())
                .addValue("orderStatus", nextState.status())
                .addValue("aggregateId", command.aggregateId())
                .addValue("expectedVersion", head.currentVersion()));
        if (updated != 1) {
            throw new OrderEventVersionConflictException(command.aggregateId(), head.currentVersion(), newVersion);
        }
        upsertMatchingState(jdbc, command, nextState);
    }

    private void upsertMatchingState(
            NamedParameterJdbcTemplate jdbc,
            OrderEventAppendCommand command,
            CommandState nextState) {
        if (nextState.userId() == null || nextState.remainingAmount() == null || nextState.status() == null) {
            return;
        }
        if (command.payload() instanceof OrderMatchedV1 matched) {
            updateMatchingStateFromEventStoreMatch(
                    jdbc,
                    command.aggregateId(),
                    matched.amount(),
                    command.eventId().toString(),
                    nextState);
            return;
        }
        if ("OrderMatchedV1".equals(command.eventType()) && command.payload() instanceof Map<?, ?> map) {
            int amount = ((Number) map.get("amount")).intValue();
            updateMatchingStateFromEventStoreMatch(
                    jdbc,
                    command.aggregateId(),
                    amount,
                    command.eventId().toString(),
                    nextState);
            return;
        }
        jdbc.update("""
                INSERT INTO order_service.order_matching_state
                    (order_id, user_id, remaining_amount, matched_amount, status, updated_at)
                VALUES
                    (:orderId, :userId, :remainingAmount, 0, :orderStatus, CURRENT_TIMESTAMP)
                ON CONFLICT (order_id) DO UPDATE
                SET user_id = EXCLUDED.user_id,
                    remaining_amount = EXCLUDED.remaining_amount,
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
                """, new MapSqlParameterSource()
                .addValue("orderId", command.aggregateId())
                .addValue("userId", nextState.userId())
                .addValue("remainingAmount", nextState.remainingAmount())
                .addValue("orderStatus", nextState.status()));
    }

    private void updateMatchingStateFromEventStoreMatch(
            NamedParameterJdbcTemplate jdbc,
            UUID orderId,
            int quantity,
            String tradeId,
            CommandState nextState) {
        int updated = jdbc.update("""
                UPDATE order_service.order_matching_state
                SET remaining_amount = :remainingAmount,
                    matched_amount = matched_amount + :quantity,
                    status = :orderStatus,
                    last_trade_id = :tradeId,
                    updated_at = CURRENT_TIMESTAMP
                WHERE order_id = :orderId
                  AND remaining_amount >= :quantity
                """, new MapSqlParameterSource()
                .addValue("orderId", orderId)
                .addValue("quantity", quantity)
                .addValue("remainingAmount", nextState.remainingAmount())
                .addValue("orderStatus", nextState.status())
                .addValue("tradeId", tradeId));
        if (updated != 1) {
            throw new IllegalStateException("Expected one order matching state update for event-store match: orderId="
                    + orderId);
        }
    }

    private CommandState nextCommandState(StreamHead head, OrderEventAppendCommand command) {
        Object payload = command.payload();
        if (payload instanceof OrderSubmissionRequestedV1 requested) {
            return new CommandState(requested.userId(), requested.amount(), "PENDING_ASSET_CHECK");
        }
        if (payload instanceof OrderAssetReservationConfirmedV1 confirmed) {
            return new CommandState(confirmed.userId(), head.remainingAmount(), "OPEN");
        }
        if (payload instanceof OrderAssetReservationFailedV1 failed) {
            return new CommandState(failed.userId(), head.remainingAmount(), "REJECTED");
        }
        if (payload instanceof OrderMatchedV1 matched) {
            int remaining = requireRemainingAmount(head) - matched.amount();
            if (remaining < 0) {
                throw new IllegalStateException("Order matched amount exceeds remaining amount: orderId="
                        + matched.orderId());
            }
            return new CommandState(head.userId(), remaining, remaining == 0 ? "MATCHED" : "PARTIALLY_MATCHED");
        }
        if (payload instanceof OrderCancelledV1 cancelled) {
            return new CommandState(cancelled.userId(), head.remainingAmount(), "CANCELLED");
        }
        if ("OrderMatchedV1".equals(command.eventType()) && payload instanceof Map<?, ?> map) {
            int amount = ((Number) map.get("amount")).intValue();
            int remaining = requireRemainingAmount(head) - amount;
            if (remaining < 0) {
                throw new IllegalStateException("Order matched amount exceeds remaining amount: orderId="
                        + command.aggregateId());
            }
            return new CommandState(head.userId(), remaining, remaining == 0 ? "MATCHED" : "PARTIALLY_MATCHED");
        }
        return new CommandState(head.userId(), head.remainingAmount(), head.status());
    }

    private int requireRemainingAmount(StreamHead head) {
        if (head.remainingAmount() == null) {
            throw new IllegalStateException("Order stream head has no remaining amount for match command");
        }
        return head.remainingAmount();
    }

    private void insertOutboxIfPresent(
            NamedParameterJdbcTemplate jdbc,
            OrderEventAppendCommand command,
            String domainPayloadCanonical) {
        OrderIntegrationEvent integration = command.integrationEvent();
        if (integration == null) {
            return;
        }
        String integrationPayload = integration.payload() == command.payload()
                ? domainPayloadCanonical
                : serialize(integration.payload());
        jdbc.update("""
                INSERT INTO order_service.order_event_outbox
                    (event_id, aggregate_id, exchange_name, routing_key, message_type, payload,
                     status, attempt_count, next_retry_at, created_at, updated_at)
                VALUES
                    (:eventId, :aggregateId, :exchange, :routingKey, :messageType, :payload,
                     'PENDING', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, new MapSqlParameterSource()
                .addValue("eventId", command.eventId())
                .addValue("aggregateId", command.aggregateId())
                .addValue("exchange", integration.exchange())
                .addValue("routingKey", integration.routingKey())
                .addValue("messageType", integration.payload().getClass().getName())
                .addValue("payload", integrationPayload));
    }

    private void insertSharedOutboxIfPresent(
            NamedParameterJdbcTemplate jdbc,
            UUID eventId,
            UUID aggregateId,
            OrderIntegrationEvent integration,
            String integrationPayload) {
        if (integration == null) {
            return;
        }
        jdbc.update("""
                INSERT INTO order_service.order_event_outbox
                    (event_id, aggregate_id, exchange_name, routing_key, message_type, payload,
                     status, attempt_count, next_retry_at, created_at, updated_at)
                VALUES
                    (:eventId, :aggregateId, :exchange, :routingKey, :messageType, :payload,
                     'PENDING', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, new MapSqlParameterSource()
                .addValue("eventId", eventId)
                .addValue("aggregateId", aggregateId)
                .addValue("exchange", integration.exchange())
                .addValue("routingKey", integration.routingKey())
                .addValue("messageType", integration.payload().getClass().getName())
                .addValue("payload", integrationPayload));
    }

    private String serialize(Object value) {
        try {
            return canonicalObjectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Event payload cannot be serialized", e);
        }
    }

    private String computeHash(
            OrderEventAppendCommand command,
            long aggregateVersion,
            String payloadCanonical,
            String metadataCanonical,
            String prevHash) {
        String material = command.eventId() + "|"
                + command.aggregateId() + "|"
                + aggregateVersion + "|"
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
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private record StreamHead(
            long currentVersion,
            String lastHash,
            UUID userId,
            Integer remainingAmount,
            String status) {

        private boolean canMatch(int quantity) {
            return ("OPEN".equals(status) || "PARTIALLY_MATCHED".equals(status))
                    && quantity > 0
                    && userId != null
                    && remainingAmount != null
                    && remainingAmount >= quantity;
        }
    }

    private record CommandState(UUID userId, Integer remainingAmount, String status) {
    }

    private record TradeApplicationHotPathOutcome(
            int insertedTradeApplications,
            int updatedMatchingStates,
            int insertedOutboxes) {
    }

    private record TradeApplicationHotPathBatchOutcome(
            int existingTradeApplications,
            int insertedTradeApplications,
            int updatedMatchingStates,
            int insertedOutboxes) {
    }

    private record TradeApplicationBatchAppendCommandWithPayload(
            TradeApplicationBatchAppendCommand command,
            String integrationPayload) {
    }

    private record TradeApplicationHotPathBatchAppend(
            UUID buyerOrderId,
            int buyerMatchedQuantity,
            MatchingState buyerState,
            UUID sellerOrderId,
            int sellerMatchedQuantity,
            MatchingState sellerState,
            OrderTradeApplication tradeApplication,
            OrderIntegrationEvent integrationEvent,
            String integrationPayload) {
    }

    private record TradeApplicationBatchArrays(
            String[] tradeIds,
            UUID[] tradeBuyerOrderIds,
            UUID[] tradeSellerOrderIds,
            Integer[] tradePrices,
            Integer[] tradeQuantities,
            Timestamp[] tradeAppliedAts,
            UUID[] buyerOrderIds,
            Integer[] buyerQuantities,
            Integer[] buyerPreviousRemainingAmounts,
            Integer[] buyerRemainingAmounts,
            Integer[] buyerMatchedAmounts,
            String[] buyerStatuses,
            UUID[] sellerOrderIds,
            Integer[] sellerQuantities,
            Integer[] sellerPreviousRemainingAmounts,
            Integer[] sellerRemainingAmounts,
            Integer[] sellerMatchedAmounts,
            String[] sellerStatuses,
            UUID[] outboxEventIds,
            UUID[] outboxAggregateIds,
            String[] outboxExchanges,
            String[] outboxRoutingKeys,
            String[] outboxMessageTypes,
            String[] outboxPayloads) {
    }

    private record MatchingState(
            UUID userId,
            int remainingAmount,
            int matchedAmount,
            String status) {

        private boolean canMatch(int quantity) {
            return ("OPEN".equals(status) || "PARTIALLY_MATCHED".equals(status))
                    && quantity > 0
                    && remainingAmount >= quantity;
        }

        private boolean canCancel() {
            return ("OPEN".equals(status) || "PARTIALLY_MATCHED".equals(status))
                    && remainingAmount > 0;
        }

        private MatchingState apply(int quantity) {
            int nextRemaining = remainingAmount - quantity;
            if (nextRemaining < 0) {
                throw new IllegalStateException("Order matched amount exceeds remaining amount");
            }
            return new MatchingState(
                    userId,
                    nextRemaining,
                    matchedAmount + quantity,
                    nextRemaining == 0 ? "MATCHED" : "PARTIALLY_MATCHED");
        }
    }

    public record TradeApplicationBatchAppendCommand(
            OrderEventAppendCommand buyerCommand,
            int buyerMatchedQuantity,
            OrderEventAppendCommand sellerCommand,
            int sellerMatchedQuantity,
            OrderTradeApplication tradeApplication,
            OrderIntegrationEvent integrationEvent) {
    }

    public record TradeApplicationBatchAppendResult(
            TradeApplicationBatchAppendStatus status,
            int appliedCount,
            TradeApplicationBatchNotBatchableReason notBatchableReason) {

        private static TradeApplicationBatchAppendResult applied(int appliedCount) {
            return new TradeApplicationBatchAppendResult(
                    TradeApplicationBatchAppendStatus.APPLIED,
                    appliedCount,
                    TradeApplicationBatchNotBatchableReason.NONE);
        }

        private static TradeApplicationBatchAppendResult notBatchable(
                TradeApplicationBatchNotBatchableReason reason) {
            return new TradeApplicationBatchAppendResult(
                    TradeApplicationBatchAppendStatus.NOT_BATCHABLE,
                    0,
                    reason);
        }
    }

    public enum TradeApplicationBatchAppendStatus {
        APPLIED,
        NOT_BATCHABLE
    }

    public enum TradeApplicationBatchNotBatchableReason {
        NONE("none"),
        OVERLAPPING_ORDER("overlapping_order"),
        MISSING_HEAD("missing_head"),
        EXISTING_TRADE_APPLICATION("existing_trade_application"),
        INVALID_HEAD_STATE("invalid_head_state");

        private final String metricName;

        TradeApplicationBatchNotBatchableReason(String metricName) {
            this.metricName = metricName;
        }

        public String metricName() {
            return metricName;
        }
    }

    public record TradeExecutionAppendResult(
            TradeExecutionAppendStatus status,
            OrderEventAppendResult appendResult) {

        private static TradeExecutionAppendResult applied(OrderEventAppendResult appendResult) {
            return new TradeExecutionAppendResult(TradeExecutionAppendStatus.APPLIED, appendResult);
        }

        private static TradeExecutionAppendResult duplicate() {
            return new TradeExecutionAppendResult(TradeExecutionAppendStatus.DUPLICATE, null);
        }

        private static TradeExecutionAppendResult notFastPath() {
            return new TradeExecutionAppendResult(TradeExecutionAppendStatus.NOT_FAST_PATH, null);
        }
    }

    public enum TradeExecutionAppendStatus {
        APPLIED,
        DUPLICATE,
        NOT_FAST_PATH
    }

    private record ExistingEvent(
            UUID aggregateId,
            long aggregateVersion,
            String eventType,
            String payloadCanonical,
            String metadataCanonical,
            int schemaVersion,
            LocalDateTime occurredAt,
            long globalPosition,
            String hash) {
    }
}
