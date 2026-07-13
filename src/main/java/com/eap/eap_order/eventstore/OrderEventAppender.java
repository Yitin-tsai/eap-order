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
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

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

    public TradeExecutionAppendResult appendMatchedFromCaughtUpProjectionIfTradeLinkAbsent(
            OrderEventAppendCommand command,
            int matchedQuantity,
            OrderTradeExecutionLink link) {
        return consumerTransactionTemplate.execute(status ->
                appendMatchedFromCaughtUpProjectionIfTradeLinkAbsentInTransaction(command, matchedQuantity, link));
    }

    public TradeExecutionAppendResult appendTradeMatchedFromCaughtUpProjectionIfTradeLinksAbsent(
            OrderEventAppendCommand buyerCommand,
            int buyerMatchedQuantity,
            OrderTradeExecutionLink buyerLink,
            OrderEventAppendCommand sellerCommand,
            int sellerMatchedQuantity,
            OrderTradeExecutionLink sellerLink,
            OrderIntegrationEvent integrationEvent) {
        return consumerTransactionTemplate.execute(status ->
                appendTradeMatchedFromCaughtUpProjectionIfTradeLinksAbsentInTransaction(
                        buyerCommand, buyerMatchedQuantity, buyerLink,
                        sellerCommand, sellerMatchedQuantity, sellerLink,
                        integrationEvent));
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

    public TradeExecutionAppendResult appendTradeMatchedFromCaughtUpProjectionWithEventStoreIdempotency(
            OrderEventAppendCommand buyerCommand,
            int buyerMatchedQuantity,
            OrderEventAppendCommand sellerCommand,
            int sellerMatchedQuantity,
            OrderIntegrationEvent integrationEvent) {
        String integrationPayload = integrationEvent == null ? null : serialize(integrationEvent.payload());
        return consumerTransactionTemplate.execute(status ->
                appendTradeMatchedFromCaughtUpProjectionWithEventStoreIdempotencyInTransaction(
                        buyerCommand, buyerMatchedQuantity,
                        sellerCommand, sellerMatchedQuantity,
                        integrationEvent,
                        integrationPayload));
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

    public TradeExecutionAppendResult appendFromConsumerIfTradeLinkAbsent(
            OrderEventAppendCommand command,
            OrderTradeExecutionLink link) {
        return consumerTransactionTemplate.execute(status ->
                appendFromConsumerIfTradeLinkAbsentInTransaction(command, link));
    }

    public TradeExecutionAppendResult appendTradeFromConsumerIfTradeLinksAbsent(
            OrderEventAppendCommand buyerCommand,
            OrderTradeExecutionLink buyerLink,
            OrderEventAppendCommand sellerCommand,
            OrderTradeExecutionLink sellerLink,
            OrderIntegrationEvent integrationEvent) {
        return consumerTransactionTemplate.execute(status ->
                appendTradeFromConsumerIfTradeLinksAbsentInTransaction(
                        buyerCommand, buyerLink, sellerCommand, sellerLink, integrationEvent));
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

    private TradeExecutionAppendResult appendMatchedFromCaughtUpProjectionIfTradeLinkAbsentInTransaction(
            OrderEventAppendCommand draftCommand,
            int matchedQuantity,
            OrderTradeExecutionLink link) {
        StreamHead head = lockHead(consumerJdbc, draftCommand.aggregateId());
        if (!head.canMatch(matchedQuantity)) {
            return TradeExecutionAppendResult.notFastPath();
        }
        if (insertExecutionLinkIfAbsent(link) == 0) {
            return TradeExecutionAppendResult.duplicate();
        }

        Map<String, Object> metadata = new HashMap<>(draftCommand.metadata());
        metadata.put("correlationId", draftCommand.aggregateId().toString());
        metadata.put("userId", head.userId().toString());
        OrderEventAppendCommand command = new OrderEventAppendCommand(
                draftCommand.aggregateId(),
                head.currentVersion(),
                draftCommand.eventId(),
                draftCommand.eventType(),
                draftCommand.payload(),
                metadata,
                draftCommand.schemaVersion(),
                draftCommand.occurredAt(),
                draftCommand.integrationEvent());
        OrderEventAppendResult result = appendInTransactionWithLockedHead(
                command,
                consumerJdbc,
                head);
        return TradeExecutionAppendResult.applied(result);
    }

    private TradeExecutionAppendResult appendTradeMatchedFromCaughtUpProjectionIfTradeLinksAbsentInTransaction(
            OrderEventAppendCommand buyerDraftCommand,
            int buyerMatchedQuantity,
            OrderTradeExecutionLink buyerLink,
            OrderEventAppendCommand sellerDraftCommand,
            int sellerMatchedQuantity,
            OrderTradeExecutionLink sellerLink,
            OrderIntegrationEvent integrationEvent) {
        validateDistinctTradeOrders(buyerDraftCommand, sellerDraftCommand);
        Map<UUID, StreamHead> heads = lockHeadsInStableOrder(
                buyerDraftCommand.aggregateId(), sellerDraftCommand.aggregateId());
        StreamHead buyerHead = heads.get(buyerDraftCommand.aggregateId());
        StreamHead sellerHead = heads.get(sellerDraftCommand.aggregateId());
        if (buyerHead == null || sellerHead == null
                || !buyerHead.canMatch(buyerMatchedQuantity)
                || !sellerHead.canMatch(sellerMatchedQuantity)) {
            return TradeExecutionAppendResult.notFastPath();
        }
        if (!insertBothExecutionLinksOrDetectDuplicate(buyerLink, sellerLink)) {
            return TradeExecutionAppendResult.duplicate();
        }

        OrderEventAppendCommand buyerCommand = withHeadState(buyerDraftCommand, buyerHead);
        OrderEventAppendCommand sellerCommand = withHeadState(sellerDraftCommand, sellerHead);
        OrderEventAppendResult buyerResult = appendMatchedTradeEventsWithLockedHeads(
                buyerCommand,
                buyerHead,
                sellerCommand,
                sellerHead);
        insertSharedOutboxIfPresent(
                consumerJdbc,
                buyerCommand.eventId(),
                buyerCommand.aggregateId(),
                integrationEvent,
                serialize(integrationEvent.payload()));
        return TradeExecutionAppendResult.applied(buyerResult);
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

    private TradeExecutionAppendResult appendTradeMatchedFromCaughtUpProjectionWithEventStoreIdempotencyInTransaction(
            OrderEventAppendCommand buyerDraftCommand,
            int buyerMatchedQuantity,
            OrderEventAppendCommand sellerDraftCommand,
            int sellerMatchedQuantity,
            OrderIntegrationEvent integrationEvent,
            String integrationPayload) {
        validateDistinctTradeOrders(buyerDraftCommand, sellerDraftCommand);
        Map<UUID, StreamHead> heads = lockHeadsInStableOrder(
                buyerDraftCommand.aggregateId(), sellerDraftCommand.aggregateId());
        StreamHead buyerHead = heads.get(buyerDraftCommand.aggregateId());
        StreamHead sellerHead = heads.get(sellerDraftCommand.aggregateId());
        OrderEventAppendCommand buyerCommand = buyerHead == null
                ? buyerDraftCommand
                : withHeadState(buyerDraftCommand, buyerHead);
        OrderEventAppendCommand sellerCommand = sellerHead == null
                ? sellerDraftCommand
                : withHeadState(sellerDraftCommand, sellerHead);
        if (buyerHead == null || sellerHead == null
                || !buyerHead.canMatch(buyerMatchedQuantity)
                || !sellerHead.canMatch(sellerMatchedQuantity)) {
            if (bothTradeEventsAlreadyExist(buyerCommand, sellerCommand)) {
                assertSharedOutboxAlreadyExists(buyerCommand, integrationEvent, integrationPayload);
                return TradeExecutionAppendResult.duplicate();
            }
            return TradeExecutionAppendResult.notFastPath();
        }

        PreparedAppend buyer = prepareAppend(buyerCommand, buyerHead);
        PreparedAppend seller = prepareAppend(sellerCommand, sellerHead);
        Map<UUID, InsertedEvent> insertedEvents = insertTradeEventsIfAbsent(buyer, seller);
        if (insertedEvents.isEmpty()) {
            if (bothTradeEventsAlreadyExist(buyer, seller)) {
                assertSharedOutboxAlreadyExists(buyer.command(), integrationEvent, integrationPayload);
                return TradeExecutionAppendResult.duplicate();
            }
            throw new IllegalStateException("Duplicate OrderMatched events did not match existing event-store rows");
        }
        if (insertedEvents.size() != 2) {
            throw new IllegalStateException("Partial Order trade event insert detected: buyerEventId="
                    + buyer.command().eventId() + ", sellerEventId=" + seller.command().eventId()
                    + ", inserted=" + insertedEvents.size());
        }
        updateTradeHeads(buyer, seller);
        insertSharedOutboxIfPresent(
                consumerJdbc,
                buyer.command().eventId(),
                buyer.command().aggregateId(),
                integrationEvent,
                integrationPayload);
        InsertedEvent buyerInserted = insertedEvents.get(buyer.command().aggregateId());
        return TradeExecutionAppendResult.applied(new OrderEventAppendResult(
                buyer.command().aggregateId(),
                buyer.command().eventId(),
                buyer.nextVersion(),
                buyerInserted.globalPosition(),
                buyer.hash(),
                false));
    }

    private TradeExecutionAppendResult appendFromConsumerIfTradeLinkAbsentInTransaction(
            OrderEventAppendCommand command,
            OrderTradeExecutionLink link) {
        if (insertExecutionLinkIfAbsent(link) == 0) {
            return TradeExecutionAppendResult.duplicate();
        }
        return TradeExecutionAppendResult.applied(appendInTransaction(command, consumerJdbc));
    }

    private TradeExecutionAppendResult appendTradeFromConsumerIfTradeLinksAbsentInTransaction(
            OrderEventAppendCommand buyerCommand,
            OrderTradeExecutionLink buyerLink,
            OrderEventAppendCommand sellerCommand,
            OrderTradeExecutionLink sellerLink,
            OrderIntegrationEvent integrationEvent) {
        validateDistinctTradeOrders(buyerCommand, sellerCommand);
        Map<UUID, StreamHead> heads = lockHeadsInStableOrder(
                buyerCommand.aggregateId(), sellerCommand.aggregateId());
        if (!insertBothExecutionLinksOrDetectDuplicate(buyerLink, sellerLink)) {
            return TradeExecutionAppendResult.duplicate();
        }
        OrderEventAppendResult buyerResult = appendInTransactionWithLockedHead(
                buyerCommand,
                consumerJdbc,
                heads.get(buyerCommand.aggregateId()));
        appendInTransactionWithLockedHead(
                sellerCommand,
                consumerJdbc,
                heads.get(sellerCommand.aggregateId()));
        insertSharedOutboxIfPresent(
                consumerJdbc,
                buyerCommand.eventId(),
                buyerCommand.aggregateId(),
                integrationEvent,
                serialize(integrationEvent.payload()));
        return TradeExecutionAppendResult.applied(buyerResult);
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

    private boolean insertBothExecutionLinksOrDetectDuplicate(
            OrderTradeExecutionLink buyerLink,
            OrderTradeExecutionLink sellerLink) {
        int inserted = insertBothExecutionLinksIfAbsent(buyerLink, sellerLink);
        if (inserted == 2) {
            return true;
        }
        if (inserted == 0 && countExecutionLinks(buyerLink.tradeId()) >= 2) {
            return false;
        }
        throw new IllegalStateException("Partial Order trade application detected: tradeId=" + buyerLink.tradeId());
    }

    private int insertBothExecutionLinksIfAbsent(
            OrderTradeExecutionLink buyerLink,
            OrderTradeExecutionLink sellerLink) {
        Integer inserted = consumerJdbc.queryForObject("""
                WITH input(trade_id, order_id, side, price, quantity, applied_at) AS (
                    VALUES
                        (:buyerTradeId, :buyerOrderId, :buyerSide, :buyerPrice, :buyerQuantity, :buyerAppliedAt),
                        (:sellerTradeId, :sellerOrderId, :sellerSide, :sellerPrice, :sellerQuantity, :sellerAppliedAt)
                ),
                inserted AS (
                    INSERT INTO order_service.order_execution_links
                        (trade_id, order_id, side, price, quantity, applied_at)
                    SELECT trade_id, order_id, side, price, quantity, applied_at
                    FROM input
                    ON CONFLICT (trade_id, order_id) DO NOTHING
                    RETURNING 1
                )
                SELECT COUNT(*) FROM inserted
                """, new MapSqlParameterSource()
                .addValue("buyerTradeId", buyerLink.tradeId())
                .addValue("buyerOrderId", buyerLink.orderId())
                .addValue("buyerSide", buyerLink.side())
                .addValue("buyerPrice", buyerLink.price())
                .addValue("buyerQuantity", buyerLink.quantity())
                .addValue("buyerAppliedAt", buyerLink.appliedAt())
                .addValue("sellerTradeId", sellerLink.tradeId())
                .addValue("sellerOrderId", sellerLink.orderId())
                .addValue("sellerSide", sellerLink.side())
                .addValue("sellerPrice", sellerLink.price())
                .addValue("sellerQuantity", sellerLink.quantity())
                .addValue("sellerAppliedAt", sellerLink.appliedAt()), Integer.class);
        return inserted == null ? 0 : inserted;
    }

    private int countExecutionLinks(String tradeId) {
        Integer count = consumerJdbc.queryForObject("""
                SELECT COUNT(*)
                FROM order_service.order_execution_links
                WHERE trade_id = :tradeId
                """, Map.of("tradeId", tradeId), Integer.class);
        return count == null ? 0 : count;
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
        StringBuilder values = new StringBuilder();
        MapSqlParameterSource params = new MapSqlParameterSource();
        for (int i = 0; i < preparedBatch.size(); i++) {
            if (i > 0) {
                values.append(",\n");
            }
            values.append("(:tradeId").append(i)
                    .append(", :tradeBuyerOrderId").append(i)
                    .append(", :tradeSellerOrderId").append(i)
                    .append(", :tradePrice").append(i)
                    .append(", :tradeQuantity").append(i)
                    .append(", :tradeAppliedAt").append(i)
                    .append(", :buyerOrderId").append(i)
                    .append(", :buyerQuantity").append(i)
                    .append(", :buyerPreviousRemainingAmount").append(i)
                    .append(", :buyerRemainingAmount").append(i)
                    .append(", :buyerMatchedAmount").append(i)
                    .append(", :buyerStatus").append(i)
                    .append(", :sellerOrderId").append(i)
                    .append(", :sellerQuantity").append(i)
                    .append(", :sellerPreviousRemainingAmount").append(i)
                    .append(", :sellerRemainingAmount").append(i)
                    .append(", :sellerMatchedAmount").append(i)
                    .append(", :sellerStatus").append(i)
                    .append(", :outboxEventId").append(i)
                    .append(", :outboxAggregateId").append(i)
                    .append(", :outboxExchange").append(i)
                    .append(", :outboxRoutingKey").append(i)
                    .append(", :outboxMessageType").append(i)
                    .append(", :outboxPayload").append(i)
                    .append(")");
            addHotPathBatchParams(params, preparedBatch.get(i), i);
        }
        final int[] existingTradeApplications = {0};
        final int[] insertedTradeApplications = {0};
        final int[] updatedMatchingStates = {0};
        final int[] insertedOutboxes = {0};
        consumerJdbc.query("""
                WITH input(trade_id, trade_buyer_order_id, trade_seller_order_id, trade_price,
                           trade_quantity, trade_applied_at,
                           buyer_order_id, buyer_quantity, buyer_previous_remaining_amount,
                           buyer_remaining_amount, buyer_matched_amount, buyer_status,
                           seller_order_id, seller_quantity, seller_previous_remaining_amount,
                           seller_remaining_amount, seller_matched_amount, seller_status,
                           outbox_event_id, outbox_aggregate_id, outbox_exchange, outbox_routing_key,
                           outbox_message_type, outbox_payload) AS (
                    VALUES
                """ + values + """
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
                """, params, rs -> {
            existingTradeApplications[0] = rs.getInt("existing_trade_applications");
            insertedTradeApplications[0] = rs.getInt("inserted_trade_applications");
            updatedMatchingStates[0] = rs.getInt("updated_matching_states");
            insertedOutboxes[0] = rs.getInt("inserted_outboxes");
        });
        return new TradeApplicationHotPathBatchOutcome(
                existingTradeApplications[0],
                insertedTradeApplications[0],
                updatedMatchingStates[0],
                insertedOutboxes[0]);
    }

    private void insertTradeApplications(List<TradeApplicationPreparedBatchAppend> preparedBatch) {
        int[] counts = consumerJdbc.batchUpdate("""
                INSERT INTO order_service.order_trade_applications
                    (trade_id, buyer_order_id, seller_order_id, price, quantity, applied_at)
                VALUES
                    (:tradeId, :buyerOrderId, :sellerOrderId, :price, :quantity, :appliedAt)
                ON CONFLICT (trade_id) DO NOTHING
                """, preparedBatch.stream()
                .map(this::tradeApplicationParams)
                .toArray(MapSqlParameterSource[]::new));
        assertBatchCount("trade application", preparedBatch.size(), counts);
    }

    private void insertPreparedEvents(List<PreparedAppend> preparedEvents) {
        int[] counts = consumerJdbc.batchUpdate("""
                INSERT INTO order_service.order_event_store
                    (event_id, aggregate_id, aggregate_type, aggregate_version,
                     event_type, payload_canonical, metadata_canonical,
                     schema_version, occurred_at, prev_hash, hash)
                VALUES
                    (:eventId, :aggregateId, :aggregateType, :aggregateVersion,
                     :eventType, :payloadCanonical, :metadataCanonical,
                     :schemaVersion, :occurredAt, :prevHash, :hash)
                """, preparedEvents.stream()
                .map(this::preparedEventParams)
                .toArray(MapSqlParameterSource[]::new));
        assertBatchCount("event-store event", preparedEvents.size(), counts);
    }

    private void updatePreparedHeads(List<PreparedAppend> preparedEvents) {
        int[] counts = consumerJdbc.batchUpdate("""
                UPDATE order_service.order_stream_heads
                SET current_version = :aggregateVersion,
                    last_event_id = :eventId,
                    last_hash = :hash,
                    user_id = :userId,
                    remaining_amount = :remainingAmount,
                    status = :orderStatus,
                    updated_at = CURRENT_TIMESTAMP
                WHERE aggregate_id = :aggregateId
                  AND current_version = :expectedVersion
                """, preparedEvents.stream()
                .map(this::preparedHeadParams)
                .toArray(MapSqlParameterSource[]::new));
        assertBatchCount("stream head", preparedEvents.size(), counts);
    }

    private void insertPreparedOutboxes(List<TradeApplicationPreparedBatchAppend> preparedBatch) {
        MapSqlParameterSource[] params = preparedBatch.stream()
                .filter(prepared -> prepared.integrationEvent() != null)
                .map(this::preparedOutboxParams)
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

    private TradeApplicationAppendOutcome insertTradeApplicationEventsHeadsAndOutbox(
            PreparedAppend buyer,
            PreparedAppend seller,
            OrderTradeApplication tradeApplication,
            OrderIntegrationEvent integrationEvent,
            String integrationPayload) {
        Map<UUID, InsertedEvent> inserted = new HashMap<>();
        final int[] insertedTradeApplications = {0};
        final int[] updatedHeads = {0};
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
                input(event_id, aggregate_id, aggregate_type, aggregate_version,
                      event_type, payload_canonical, metadata_canonical,
                      schema_version, occurred_at, prev_hash, hash,
                      expected_version, user_id, remaining_amount, order_status) AS (
                    VALUES
                        (:buyerEventId, :buyerAggregateId, :aggregateType, :buyerAggregateVersion,
                         :buyerEventType, :buyerPayloadCanonical,
                         :buyerMetadataCanonical,
                         :buyerSchemaVersion, :buyerOccurredAt, :buyerPrevHash, :buyerHash,
                         :buyerExpectedVersion, :buyerUserId, :buyerRemainingAmount, :buyerStatus),
                        (:sellerEventId, :sellerAggregateId, :aggregateType, :sellerAggregateVersion,
                         :sellerEventType, :sellerPayloadCanonical,
                         :sellerMetadataCanonical,
                         :sellerSchemaVersion, :sellerOccurredAt, :sellerPrevHash, :sellerHash,
                         :sellerExpectedVersion, :sellerUserId, :sellerRemainingAmount, :sellerStatus)
                ),
                inserted_events AS (
                    INSERT INTO order_service.order_event_store
                        (event_id, aggregate_id, aggregate_type, aggregate_version,
                         event_type, payload_canonical, metadata_canonical,
                         schema_version, occurred_at, prev_hash, hash)
                    SELECT event_id, aggregate_id, aggregate_type, aggregate_version,
                           event_type, payload_canonical, metadata_canonical,
                           schema_version, occurred_at, prev_hash, hash
                    FROM input
                    WHERE EXISTS (SELECT 1 FROM trade_application)
                    RETURNING aggregate_id, aggregate_version, global_position, hash
                ),
                updated_heads AS (
                    UPDATE order_service.order_stream_heads head
                    SET current_version = input.aggregate_version,
                        last_event_id = input.event_id,
                        last_hash = input.hash,
                        user_id = input.user_id,
                        remaining_amount = input.remaining_amount,
                        status = input.order_status,
                        updated_at = CURRENT_TIMESTAMP
                    FROM input
                    JOIN inserted_events ON inserted_events.aggregate_id = input.aggregate_id
                    WHERE head.aggregate_id = input.aggregate_id
                      AND head.current_version = input.expected_version
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
                ),
                counts AS (
                    SELECT
                        (SELECT COUNT(*) FROM trade_application) AS inserted_trade_applications,
                        (SELECT COUNT(*) FROM updated_heads) AS updated_heads,
                        (SELECT COUNT(*) FROM inserted_outbox) AS inserted_outboxes
                )
                SELECT inserted_events.aggregate_id, inserted_events.aggregate_version,
                       inserted_events.global_position, inserted_events.hash,
                       counts.inserted_trade_applications, counts.updated_heads, counts.inserted_outboxes
                FROM counts
                LEFT JOIN inserted_events ON TRUE
                """, tradeApplicationAppendParams(
                buyer,
                seller,
                tradeApplication,
                integrationEvent,
                integrationPayload), rs -> {
            insertedTradeApplications[0] = rs.getInt("inserted_trade_applications");
            updatedHeads[0] = rs.getInt("updated_heads");
            insertedOutboxes[0] = rs.getInt("inserted_outboxes");
            UUID aggregateId = rs.getObject("aggregate_id", UUID.class);
            if (aggregateId != null) {
                inserted.put(
                        aggregateId,
                        new InsertedEvent(
                                rs.getLong("aggregate_version"),
                                rs.getLong("global_position"),
                                rs.getString("hash")));
            }
        });
        return new TradeApplicationAppendOutcome(
                inserted,
                insertedTradeApplications[0],
                updatedHeads[0],
                insertedOutboxes[0]);
    }

    private OrderEventAppendCommand withHeadState(
            OrderEventAppendCommand draftCommand,
            StreamHead head) {
        Map<String, Object> metadata = new HashMap<>(draftCommand.metadata());
        metadata.put("correlationId", draftCommand.aggregateId().toString());
        metadata.put("userId", head.userId().toString());
        return new OrderEventAppendCommand(
                draftCommand.aggregateId(),
                head.currentVersion(),
                draftCommand.eventId(),
                draftCommand.eventType(),
                draftCommand.payload(),
                metadata,
                draftCommand.schemaVersion(),
                draftCommand.occurredAt(),
                draftCommand.integrationEvent());
    }

    private int insertExecutionLinkIfAbsent(OrderTradeExecutionLink link) {
        return consumerJdbc.update("""
                INSERT INTO order_service.order_execution_links
                    (trade_id, order_id, side, price, quantity, applied_at)
                VALUES
                    (:tradeId, :orderId, :side, :price, :quantity, :appliedAt)
                ON CONFLICT (trade_id, order_id) DO NOTHING
                """, new MapSqlParameterSource()
                .addValue("tradeId", link.tradeId())
                .addValue("orderId", link.orderId())
                .addValue("side", link.side())
                .addValue("price", link.price())
                .addValue("quantity", link.quantity())
                .addValue("appliedAt", link.appliedAt()));
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

    private OrderEventAppendResult appendMatchedTradeEventsWithLockedHeads(
            OrderEventAppendCommand buyerCommand,
            StreamHead buyerHead,
            OrderEventAppendCommand sellerCommand,
            StreamHead sellerHead) {
        PreparedAppend buyer = prepareAppend(buyerCommand, buyerHead);
        PreparedAppend seller = prepareAppend(sellerCommand, sellerHead);
        Map<UUID, InsertedEvent> insertedEvents = insertTradeEventsAndUpdateHeads(buyer, seller);
        InsertedEvent buyerInserted = insertedEvents.get(buyer.command().aggregateId());
        if (buyerInserted == null) {
            throw new IllegalStateException("Buyer OrderMatched event was not inserted: orderId="
                    + buyer.command().aggregateId());
        }
        return new OrderEventAppendResult(
                buyer.command().aggregateId(),
                buyer.command().eventId(),
                buyer.nextVersion(),
                buyerInserted.globalPosition(),
                buyer.hash(),
                false);
    }

    private PreparedAppend prepareAppend(OrderEventAppendCommand command, StreamHead head) {
        String payloadCanonical = serialize(command.payload());
        String metadataCanonical = serialize(command.metadata());
        if (head.currentVersion() != command.expectedVersion()) {
            throw new OrderEventVersionConflictException(
                    command.aggregateId(), command.expectedVersion(), head.currentVersion());
        }
        long nextVersion = head.currentVersion() + 1;
        String hash = computeHash(
                command,
                nextVersion,
                payloadCanonical,
                metadataCanonical,
                head.lastHash());
        CommandState nextState = nextCommandState(head, command);
        return new PreparedAppend(
                command,
                head,
                nextVersion,
                payloadCanonical,
                metadataCanonical,
                hash,
                nextState);
    }

    private Map<UUID, InsertedEvent> insertTradeEvents(PreparedAppend buyer, PreparedAppend seller) {
        Map<UUID, InsertedEvent> inserted = new HashMap<>();
        consumerJdbc.query("""
                WITH input(event_id, aggregate_id, aggregate_type, aggregate_version,
                           event_type, payload_canonical, metadata_canonical,
                           schema_version, occurred_at, prev_hash, hash) AS (
                    VALUES
                        (:buyerEventId, :buyerAggregateId, :aggregateType, :buyerAggregateVersion,
                         :buyerEventType, :buyerPayloadCanonical,
                         :buyerMetadataCanonical,
                         :buyerSchemaVersion, :buyerOccurredAt, :buyerPrevHash, :buyerHash),
                        (:sellerEventId, :sellerAggregateId, :aggregateType, :sellerAggregateVersion,
                         :sellerEventType, :sellerPayloadCanonical,
                         :sellerMetadataCanonical,
                         :sellerSchemaVersion, :sellerOccurredAt, :sellerPrevHash, :sellerHash)
                )
                INSERT INTO order_service.order_event_store
                    (event_id, aggregate_id, aggregate_type, aggregate_version,
                     event_type, payload_canonical, metadata_canonical,
                     schema_version, occurred_at, prev_hash, hash)
                SELECT event_id, aggregate_id, aggregate_type, aggregate_version,
                       event_type, payload_canonical, metadata_canonical,
                       schema_version, occurred_at, prev_hash, hash
                FROM input
                RETURNING aggregate_id, aggregate_version, global_position, hash
                """, tradeAppendParams(buyer, seller), rs -> {
            inserted.put(
                    rs.getObject("aggregate_id", UUID.class),
                    new InsertedEvent(
                            rs.getLong("aggregate_version"),
                            rs.getLong("global_position"),
                            rs.getString("hash")));
        });
        if (inserted.size() != 2) {
            throw new IllegalStateException("Expected two OrderMatched events to be inserted, actual="
                    + inserted.size());
        }
        return inserted;
    }

    private Map<UUID, InsertedEvent> insertTradeEventsAndUpdateHeads(PreparedAppend buyer, PreparedAppend seller) {
        Map<UUID, InsertedEvent> inserted = new HashMap<>();
        final int[] updatedHeads = {0};
        consumerJdbc.query("""
                WITH input(event_id, aggregate_id, aggregate_type, aggregate_version,
                           event_type, payload_canonical, metadata_canonical,
                           schema_version, occurred_at, prev_hash, hash,
                           expected_version, user_id, remaining_amount, order_status) AS (
                    VALUES
                        (:buyerEventId, :buyerAggregateId, :aggregateType, :buyerAggregateVersion,
                         :buyerEventType, :buyerPayloadCanonical,
                         :buyerMetadataCanonical,
                         :buyerSchemaVersion, :buyerOccurredAt, :buyerPrevHash, :buyerHash,
                         :buyerExpectedVersion, :buyerUserId, :buyerRemainingAmount, :buyerStatus),
                        (:sellerEventId, :sellerAggregateId, :aggregateType, :sellerAggregateVersion,
                         :sellerEventType, :sellerPayloadCanonical,
                         :sellerMetadataCanonical,
                         :sellerSchemaVersion, :sellerOccurredAt, :sellerPrevHash, :sellerHash,
                         :sellerExpectedVersion, :sellerUserId, :sellerRemainingAmount, :sellerStatus)
                ),
                inserted AS (
                    INSERT INTO order_service.order_event_store
                        (event_id, aggregate_id, aggregate_type, aggregate_version,
                         event_type, payload_canonical, metadata_canonical,
                         schema_version, occurred_at, prev_hash, hash)
                    SELECT event_id, aggregate_id, aggregate_type, aggregate_version,
                           event_type, payload_canonical, metadata_canonical,
                           schema_version, occurred_at, prev_hash, hash
                    FROM input
                    RETURNING aggregate_id, aggregate_version, global_position, hash
                ),
                updated AS (
                    UPDATE order_service.order_stream_heads head
                    SET current_version = input.aggregate_version,
                        last_event_id = input.event_id,
                        last_hash = input.hash,
                        user_id = input.user_id,
                        remaining_amount = input.remaining_amount,
                        status = input.order_status,
                        updated_at = CURRENT_TIMESTAMP
                    FROM input
                    JOIN inserted ON inserted.aggregate_id = input.aggregate_id
                    WHERE head.aggregate_id = input.aggregate_id
                      AND head.current_version = input.expected_version
                    RETURNING 1
                ),
                updated_count AS (
                    SELECT COUNT(*) AS count FROM updated
                )
                SELECT inserted.aggregate_id, inserted.aggregate_version, inserted.global_position,
                       inserted.hash, updated_count.count AS updated_heads
                FROM inserted CROSS JOIN updated_count
                """, tradeAppendParams(buyer, seller), rs -> {
            inserted.put(
                    rs.getObject("aggregate_id", UUID.class),
                    new InsertedEvent(
                            rs.getLong("aggregate_version"),
                            rs.getLong("global_position"),
                            rs.getString("hash")));
            updatedHeads[0] = rs.getInt("updated_heads");
        });
        if (inserted.size() != 2) {
            throw new IllegalStateException("Expected two OrderMatched events to be inserted, actual="
                    + inserted.size());
        }
        if (updatedHeads[0] != 2) {
            throw new OrderEventVersionConflictException(
                    buyer.command().aggregateId(), buyer.head().currentVersion(), buyer.nextVersion());
        }
        return inserted;
    }

    private Map<UUID, InsertedEvent> insertTradeEventsIfAbsent(PreparedAppend buyer, PreparedAppend seller) {
        Map<UUID, InsertedEvent> inserted = new HashMap<>();
        consumerJdbc.query("""
                WITH input(event_id, aggregate_id, aggregate_type, aggregate_version,
                           event_type, payload_canonical, metadata_canonical,
                           schema_version, occurred_at, prev_hash, hash) AS (
                    VALUES
                        (:buyerEventId, :buyerAggregateId, :aggregateType, :buyerAggregateVersion,
                         :buyerEventType, :buyerPayloadCanonical,
                         :buyerMetadataCanonical,
                         :buyerSchemaVersion, :buyerOccurredAt, :buyerPrevHash, :buyerHash),
                        (:sellerEventId, :sellerAggregateId, :aggregateType, :sellerAggregateVersion,
                         :sellerEventType, :sellerPayloadCanonical,
                         :sellerMetadataCanonical,
                         :sellerSchemaVersion, :sellerOccurredAt, :sellerPrevHash, :sellerHash)
                )
                INSERT INTO order_service.order_event_store
                    (event_id, aggregate_id, aggregate_type, aggregate_version,
                     event_type, payload_canonical, metadata_canonical,
                     schema_version, occurred_at, prev_hash, hash)
                SELECT event_id, aggregate_id, aggregate_type, aggregate_version,
                       event_type, payload_canonical, metadata_canonical,
                       schema_version, occurred_at, prev_hash, hash
                FROM input
                ON CONFLICT (event_id) DO NOTHING
                RETURNING aggregate_id, aggregate_version, global_position, hash
                """, tradeAppendParams(buyer, seller), rs -> {
            inserted.put(
                    rs.getObject("aggregate_id", UUID.class),
                    new InsertedEvent(
                            rs.getLong("aggregate_version"),
                            rs.getLong("global_position"),
                            rs.getString("hash")));
        });
        return inserted;
    }

    private boolean bothTradeEventsAlreadyExist(
            OrderEventAppendCommand buyerCommand,
            OrderEventAppendCommand sellerCommand) {
        StreamHead buyerHead = new StreamHead(0, GENESIS_HASH, null, null, null);
        StreamHead sellerHead = new StreamHead(0, GENESIS_HASH, null, null, null);
        return bothTradeEventsAlreadyExist(
                prepareDuplicateCheckAppend(buyerCommand, buyerHead),
                prepareDuplicateCheckAppend(sellerCommand, sellerHead));
    }

    private boolean bothTradeEventsAlreadyExist(PreparedAppend buyer, PreparedAppend seller) {
        return existingEventMatches(buyer) && existingEventMatches(seller);
    }

    private PreparedAppend prepareDuplicateCheckAppend(OrderEventAppendCommand command, StreamHead fallbackHead) {
        String payloadCanonical = serialize(command.payload());
        String metadataCanonical = serialize(command.metadata());
        return new PreparedAppend(
                command,
                fallbackHead,
                0,
                payloadCanonical,
                metadataCanonical,
                "",
                new CommandState(null, null, null));
    }

    private boolean existingEventMatches(PreparedAppend append) {
        ExistingEvent existing = findByEventId(consumerJdbc, append.command().eventId());
        if (existing == null) {
            return false;
        }
        existingAppendResult(
                append.command(),
                existing,
                append.payloadCanonical(),
                append.metadataCanonical());
        return true;
    }

    private void assertSharedOutboxAlreadyExists(
            OrderEventAppendCommand buyerCommand,
            OrderIntegrationEvent integration,
            String integrationPayload) {
        if (integration == null) {
            return;
        }
        Integer count = consumerJdbc.queryForObject("""
                SELECT COUNT(*)
                FROM order_service.order_event_outbox
                WHERE event_id = :eventId
                  AND aggregate_id = :aggregateId
                  AND exchange_name = :exchange
                  AND routing_key = :routingKey
                  AND message_type = :messageType
                  AND payload = :payload
                """, new MapSqlParameterSource()
                .addValue("eventId", buyerCommand.eventId())
                .addValue("aggregateId", buyerCommand.aggregateId())
                .addValue("exchange", integration.exchange())
                .addValue("routingKey", integration.routingKey())
                .addValue("messageType", integration.payload().getClass().getName())
                .addValue("payload", integrationPayload), Integer.class);
        if (count == null || count != 1) {
            throw new IllegalStateException("Duplicate Order trade events are missing matching shared outbox: buyerEventId="
                    + buyerCommand.eventId());
        }
    }

    private void updateTradeHeads(PreparedAppend buyer, PreparedAppend seller) {
        Integer updated = consumerJdbc.queryForObject("""
                WITH input(aggregate_id, expected_version, new_version, event_id, hash,
                           user_id, remaining_amount, order_status) AS (
                    VALUES
                        (:buyerAggregateId, :buyerExpectedVersion, :buyerAggregateVersion,
                         :buyerEventId, :buyerHash, :buyerUserId, :buyerRemainingAmount, :buyerStatus),
                        (:sellerAggregateId, :sellerExpectedVersion, :sellerAggregateVersion,
                         :sellerEventId, :sellerHash, :sellerUserId, :sellerRemainingAmount, :sellerStatus)
                ),
                updated AS (
                    UPDATE order_service.order_stream_heads head
                    SET current_version = input.new_version,
                        last_event_id = input.event_id,
                        last_hash = input.hash,
                        user_id = input.user_id,
                        remaining_amount = input.remaining_amount,
                        status = input.order_status,
                        updated_at = CURRENT_TIMESTAMP
                    FROM input
                    WHERE head.aggregate_id = input.aggregate_id
                      AND head.current_version = input.expected_version
                    RETURNING 1
                )
                SELECT COUNT(*) FROM updated
                """, tradeAppendParams(buyer, seller), Integer.class);
        if (updated == null || updated != 2) {
            throw new OrderEventVersionConflictException(
                    buyer.command().aggregateId(), buyer.head().currentVersion(), buyer.nextVersion());
        }
    }

    private MapSqlParameterSource tradeApplicationParams(TradeApplicationPreparedBatchAppend prepared) {
        return new MapSqlParameterSource()
                .addValue("tradeId", prepared.tradeApplication().tradeId())
                .addValue("buyerOrderId", prepared.tradeApplication().buyerOrderId())
                .addValue("sellerOrderId", prepared.tradeApplication().sellerOrderId())
                .addValue("price", prepared.tradeApplication().price())
                .addValue("quantity", prepared.tradeApplication().quantity())
                .addValue("appliedAt", prepared.tradeApplication().appliedAt());
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

    private MapSqlParameterSource preparedEventParams(PreparedAppend prepared) {
        return new MapSqlParameterSource()
                .addValue("eventId", prepared.command().eventId())
                .addValue("aggregateId", prepared.command().aggregateId())
                .addValue("aggregateType", AGGREGATE_TYPE)
                .addValue("aggregateVersion", prepared.nextVersion())
                .addValue("eventType", prepared.command().eventType())
                .addValue("payloadCanonical", prepared.payloadCanonical())
                .addValue("metadataCanonical", prepared.metadataCanonical())
                .addValue("schemaVersion", prepared.command().schemaVersion())
                .addValue("occurredAt", prepared.command().occurredAt())
                .addValue("prevHash", prepared.head().lastHash())
                .addValue("hash", prepared.hash());
    }

    private MapSqlParameterSource preparedHeadParams(PreparedAppend prepared) {
        return new MapSqlParameterSource()
                .addValue("aggregateId", prepared.command().aggregateId())
                .addValue("expectedVersion", prepared.head().currentVersion())
                .addValue("aggregateVersion", prepared.nextVersion())
                .addValue("eventId", prepared.command().eventId())
                .addValue("hash", prepared.hash())
                .addValue("userId", prepared.nextState().userId())
                .addValue("remainingAmount", prepared.nextState().remainingAmount())
                .addValue("orderStatus", prepared.nextState().status());
    }

    private MapSqlParameterSource preparedOutboxParams(TradeApplicationPreparedBatchAppend prepared) {
        OrderIntegrationEvent integrationEvent = prepared.integrationEvent();
        return new MapSqlParameterSource()
                .addValue("eventId", prepared.buyer().command().eventId())
                .addValue("aggregateId", prepared.buyer().command().aggregateId())
                .addValue("exchange", integrationEvent.exchange())
                .addValue("routingKey", integrationEvent.routingKey())
                .addValue("messageType", integrationEvent.payload().getClass().getName())
                .addValue("payload", prepared.integrationPayload());
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

    private MapSqlParameterSource tradeAppendParams(PreparedAppend buyer, PreparedAppend seller) {
        return new MapSqlParameterSource()
                .addValue("aggregateType", AGGREGATE_TYPE)
                .addValue("buyerEventId", buyer.command().eventId())
                .addValue("buyerAggregateId", buyer.command().aggregateId())
                .addValue("buyerExpectedVersion", buyer.head().currentVersion())
                .addValue("buyerAggregateVersion", buyer.nextVersion())
                .addValue("buyerEventType", buyer.command().eventType())
                .addValue("buyerPayloadCanonical", buyer.payloadCanonical())
                .addValue("buyerMetadataCanonical", buyer.metadataCanonical())
                .addValue("buyerSchemaVersion", buyer.command().schemaVersion())
                .addValue("buyerOccurredAt", buyer.command().occurredAt())
                .addValue("buyerPrevHash", buyer.head().lastHash())
                .addValue("buyerHash", buyer.hash())
                .addValue("buyerUserId", buyer.nextState().userId())
                .addValue("buyerRemainingAmount", buyer.nextState().remainingAmount())
                .addValue("buyerStatus", buyer.nextState().status())
                .addValue("sellerEventId", seller.command().eventId())
                .addValue("sellerAggregateId", seller.command().aggregateId())
                .addValue("sellerExpectedVersion", seller.head().currentVersion())
                .addValue("sellerAggregateVersion", seller.nextVersion())
                .addValue("sellerEventType", seller.command().eventType())
                .addValue("sellerPayloadCanonical", seller.payloadCanonical())
                .addValue("sellerMetadataCanonical", seller.metadataCanonical())
                .addValue("sellerSchemaVersion", seller.command().schemaVersion())
                .addValue("sellerOccurredAt", seller.command().occurredAt())
                .addValue("sellerPrevHash", seller.head().lastHash())
                .addValue("sellerHash", seller.hash())
                .addValue("sellerUserId", seller.nextState().userId())
                .addValue("sellerRemainingAmount", seller.nextState().remainingAmount())
                .addValue("sellerStatus", seller.nextState().status());
    }

    private MapSqlParameterSource tradeApplicationAppendParams(
            PreparedAppend buyer,
            PreparedAppend seller,
            OrderTradeApplication tradeApplication,
            OrderIntegrationEvent integrationEvent,
            String integrationPayload) {
        MapSqlParameterSource params = tradeAppendParams(buyer, seller)
                .addValue("tradeId", tradeApplication.tradeId())
                .addValue("tradeBuyerOrderId", tradeApplication.buyerOrderId())
                .addValue("tradeSellerOrderId", tradeApplication.sellerOrderId())
                .addValue("tradePrice", tradeApplication.price())
                .addValue("tradeQuantity", tradeApplication.quantity())
                .addValue("tradeAppliedAt", tradeApplication.appliedAt());
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
                .addValue("outboxEventId", buyer.command().eventId())
                .addValue("outboxAggregateId", buyer.command().aggregateId())
                .addValue("outboxExchange", integrationEvent.exchange())
                .addValue("outboxRoutingKey", integrationEvent.routingKey())
                .addValue("outboxMessageType", integrationEvent.payload().getClass().getName())
                .addValue("outboxPayload", integrationPayload);
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

    private record PreparedAppend(
            OrderEventAppendCommand command,
            StreamHead head,
            long nextVersion,
            String payloadCanonical,
            String metadataCanonical,
            String hash,
            CommandState nextState) {
    }

    private record InsertedEvent(long aggregateVersion, long globalPosition, String hash) {
    }

    private record TradeApplicationAppendOutcome(
            Map<UUID, InsertedEvent> insertedEvents,
            int insertedTradeApplications,
            int updatedHeads,
            int insertedOutboxes) {
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

    private record TradeApplicationPreparedBatchAppend(
            PreparedAppend buyer,
            PreparedAppend seller,
            OrderTradeApplication tradeApplication,
            OrderIntegrationEvent integrationEvent,
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
