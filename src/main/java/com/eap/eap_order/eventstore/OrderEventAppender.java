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
import java.util.HexFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    public OrderEventAppender(
            @Qualifier("namedParameterJdbcTemplate") NamedParameterJdbcTemplate commandJdbc,
            @Qualifier("orderConsumerNamedParameterJdbcTemplate") NamedParameterJdbcTemplate consumerJdbc,
            @Qualifier("transactionManager") PlatformTransactionManager transactionManager,
            @Qualifier("orderConsumerTransactionManager") PlatformTransactionManager consumerTransactionManager,
            ObjectMapper objectMapper) {
        this.commandJdbc = commandJdbc;
        this.consumerJdbc = consumerJdbc;
        this.commandTransactionTemplate = new TransactionTemplate(transactionManager);
        this.consumerTransactionTemplate = new TransactionTemplate(consumerTransactionManager);
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
        OrderEventAppendResult buyerResult = appendInTransactionWithLockedHead(
                buyerCommand,
                consumerJdbc,
                buyerHead);
        appendInTransactionWithLockedHead(
                sellerCommand,
                consumerJdbc,
                sellerHead);
        insertSharedOutboxIfPresent(
                consumerJdbc,
                buyerCommand.eventId(),
                buyerCommand.aggregateId(),
                integrationEvent,
                serialize(integrationEvent.payload()));
        return TradeExecutionAppendResult.applied(buyerResult);
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
        Map<UUID, StreamHead> heads = new HashMap<>();
        List<UUID> aggregateIds = stableOrder(first, second);
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
                     event_type, payload, payload_canonical, metadata, metadata_canonical,
                     schema_version, occurred_at, prev_hash, hash)
                VALUES
                    (:eventId, :aggregateId, :aggregateType, :aggregateVersion,
                     :eventType, CAST(:payload AS jsonb), :payload,
                     CAST(:metadata AS jsonb), :metadata,
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
                    (:eventId, :aggregateId, :exchange, :routingKey, :messageType, CAST(:payload AS jsonb),
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
                    (:eventId, :aggregateId, :exchange, :routingKey, :messageType, CAST(:payload AS jsonb),
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
