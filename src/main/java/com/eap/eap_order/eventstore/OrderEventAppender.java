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
import org.springframework.beans.factory.annotation.Value;
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
    private static final String INSERT_TRADE_APPLICATIONS_AND_MATCHING_STATES_SQL = """
            WITH input(trade_id, trade_buyer_order_id, trade_seller_order_id, trade_price,
                       trade_quantity, trade_applied_at,
                       buyer_order_id, buyer_quantity, buyer_previous_remaining_amount,
                       buyer_remaining_amount, buyer_matched_amount, buyer_status,
                       seller_order_id, seller_quantity, seller_previous_remaining_amount,
                       seller_remaining_amount, seller_matched_amount, seller_status) AS (
                SELECT *
                FROM unnest(?::varchar[], ?::uuid[], ?::uuid[], ?::integer[],
                            ?::integer[], ?::timestamp[],
                            ?::uuid[], ?::integer[], ?::integer[],
                            ?::integer[], ?::integer[], ?::varchar[],
                            ?::uuid[], ?::integer[], ?::integer[],
                            ?::integer[], ?::integer[], ?::varchar[])
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
            )
            SELECT
                (SELECT count FROM existing_trade_applications) AS existing_trade_applications,
                (SELECT COUNT(*) FROM trade_application) AS inserted_trade_applications,
                (SELECT COUNT(*) FROM updated_matching_states) AS updated_matching_states
            """;
    private static final String APPEND_ASSET_RESERVATION_CONFIRMED_SINGLE_ROUND_TRIP_SQL = """
            WITH input(event_id, aggregate_id, event_type, payload_canonical, metadata_canonical,
                       schema_version, occurred_at, hash_material_prefix, current_version, new_version,
                       user_id, order_status) AS (
                SELECT *
                FROM unnest(?::uuid[], ?::uuid[], ?::varchar[], ?::text[], ?::text[],
                            ?::integer[], ?::timestamp[], ?::text[], ?::bigint[], ?::bigint[],
                            ?::uuid[], ?::varchar[])
            ),
            locked_heads AS MATERIALIZED (
                SELECT head.aggregate_id,
                       head.current_version,
                       head.last_hash,
                       head.remaining_amount,
                       input.current_version AS expected_version
                FROM order_service.order_stream_heads head
                JOIN input ON input.aggregate_id = head.aggregate_id
                ORDER BY head.aggregate_id
                FOR UPDATE OF head
            ),
            batch_eligibility AS MATERIALIZED (
                SELECT COUNT(*) = (SELECT COUNT(*) FROM input)
                       AND COALESCE(BOOL_AND(current_version = expected_version), false)
                           AS eligible
                FROM locked_heads
            ),
            prepared AS MATERIALIZED (
                SELECT input.event_id,
                       input.aggregate_id,
                       input.event_type,
                       input.payload_canonical,
                       input.metadata_canonical,
                       input.schema_version,
                       input.occurred_at,
                       locked_heads.last_hash AS prev_hash,
                       encode(
                           sha256(convert_to(input.hash_material_prefix || locked_heads.last_hash, 'UTF8')),
                           'hex'
                       ) AS hash,
                       input.current_version,
                       input.new_version,
                       input.user_id,
                       locked_heads.remaining_amount,
                       input.order_status
                FROM input
                JOIN locked_heads ON locked_heads.aggregate_id = input.aggregate_id
                CROSS JOIN batch_eligibility
                WHERE batch_eligibility.eligible
            ),
            inserted_events AS (
                INSERT INTO order_service.order_event_store
                    (event_id, aggregate_id, aggregate_type, aggregate_version,
                     event_type, payload_canonical, metadata_canonical, schema_version,
                     occurred_at, prev_hash, hash)
                SELECT event_id, aggregate_id, 'Order', new_version,
                       event_type, payload_canonical, metadata_canonical, schema_version,
                       occurred_at, prev_hash, hash
                FROM prepared
                ON CONFLICT (event_id) DO NOTHING
                RETURNING aggregate_id
            ),
            updated_heads AS (
                UPDATE order_service.order_stream_heads head
                SET current_version = prepared.new_version,
                    last_event_id = prepared.event_id,
                    last_hash = prepared.hash,
                    user_id = prepared.user_id,
                    remaining_amount = prepared.remaining_amount,
                    status = prepared.order_status,
                    updated_at = CURRENT_TIMESTAMP
                FROM prepared
                WHERE head.aggregate_id = prepared.aggregate_id
                  AND head.current_version = prepared.current_version
                RETURNING head.aggregate_id
            ),
            upserted_matching_state AS (
                INSERT INTO order_service.order_matching_state
                    (order_id, user_id, remaining_amount, matched_amount, status, updated_at)
                SELECT aggregate_id, user_id, remaining_amount, 0, order_status, CURRENT_TIMESTAMP
                FROM prepared
                ON CONFLICT (order_id) DO UPDATE
                SET user_id = EXCLUDED.user_id,
                    remaining_amount = EXCLUDED.remaining_amount,
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING order_id
            )
            SELECT
                (SELECT eligible FROM batch_eligibility) AS eligible,
                (SELECT COUNT(*) FROM inserted_events) AS inserted_events,
                (SELECT COUNT(*) FROM updated_heads) AS updated_heads,
                (SELECT COUNT(*) FROM upserted_matching_state) AS upserted_matching_state
            """;

    private final NamedParameterJdbcTemplate commandJdbc;
    private final NamedParameterJdbcTemplate consumerJdbc;
    private final TransactionTemplate commandTransactionTemplate;
    private final TransactionTemplate consumerTransactionTemplate;
    private final ObjectMapper canonicalObjectMapper;
    private final OrderTradeApplyMetrics tradeApplyMetrics;
    private final OrderSubmissionAppendMetrics submissionAppendMetrics;
    private final OrderAssetReservationAppendMetrics assetReservationAppendMetrics;
    private final SubmissionWriteMode submissionWriteMode;
    private final boolean assetReservationConfirmedSingleRoundTripEnabled;

    public OrderEventAppender(
            @Qualifier("namedParameterJdbcTemplate") NamedParameterJdbcTemplate commandJdbc,
            @Qualifier("orderConsumerNamedParameterJdbcTemplate") NamedParameterJdbcTemplate consumerJdbc,
            @Qualifier("orderCommandTransactionManager") PlatformTransactionManager transactionManager,
            @Qualifier("orderConsumerTransactionManager") PlatformTransactionManager consumerTransactionManager,
            ObjectMapper objectMapper,
            OrderTradeApplyMetrics tradeApplyMetrics,
            OrderSubmissionAppendMetrics submissionAppendMetrics,
            OrderAssetReservationAppendMetrics assetReservationAppendMetrics,
            @Value("${eap.order.submission.write-mode:current_order_path}") String submissionWriteMode,
            @Value("${eap.order.listeners.asset-reservation-confirmed.single-round-trip-enabled:false}")
            boolean assetReservationConfirmedSingleRoundTripEnabled) {
        this.commandJdbc = commandJdbc;
        this.consumerJdbc = consumerJdbc;
        this.commandTransactionTemplate = new TransactionTemplate(transactionManager);
        this.consumerTransactionTemplate = new TransactionTemplate(consumerTransactionManager);
        this.tradeApplyMetrics = tradeApplyMetrics;
        this.submissionAppendMetrics = submissionAppendMetrics;
        this.assetReservationAppendMetrics = assetReservationAppendMetrics;
        this.submissionWriteMode = SubmissionWriteMode.parse(submissionWriteMode);
        this.assetReservationConfirmedSingleRoundTripEnabled =
                assetReservationConfirmedSingleRoundTripEnabled;
        this.canonicalObjectMapper = objectMapper.copy()
                .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    public OrderEventAppendResult append(OrderEventAppendCommand command) {
        boolean recordSubmissionAppend = command.payload() instanceof OrderSubmissionRequestedV1;
        long startedNanos = System.nanoTime();
        long[] callbackStartedNanos = new long[1];
        long[] bodyCompletedNanos = new long[1];
        try {
            return commandTransactionTemplate.execute(status -> {
                callbackStartedNanos[0] = System.nanoTime();
                if (recordSubmissionAppend) {
                    submissionAppendMetrics.recordNanos(
                            "transaction_before_callback",
                            callbackStartedNanos[0] - startedNanos);
                }
                long bodyStartedNanos = System.nanoTime();
                try {
                    return appendInTransaction(command, commandJdbc, recordSubmissionAppend);
                } finally {
                    bodyCompletedNanos[0] = System.nanoTime();
                    if (recordSubmissionAppend) {
                        submissionAppendMetrics.recordNanos(
                                "transaction_body",
                                bodyCompletedNanos[0] - bodyStartedNanos);
                    }
                }
            });
        } finally {
            if (recordSubmissionAppend) {
                long completedNanos = System.nanoTime();
                submissionAppendMetrics.recordNanos("transaction_total", completedNanos - startedNanos);
                if (bodyCompletedNanos[0] > 0) {
                    submissionAppendMetrics.recordNanos(
                            "transaction_after_body",
                            completedNanos - bodyCompletedNanos[0]);
                }
            }
        }
    }

    public OrderEventAppendResult appendFromConsumer(OrderEventAppendCommand command) {
        return consumerTransactionTemplate.execute(status -> appendInTransaction(command, consumerJdbc));
    }

    public void appendFromConsumerBatch(List<OrderEventAppendCommand> commands) {
        if (commands == null || commands.isEmpty()) {
            return;
        }
        if (isAssetReservationConfirmedBatch(commands)) {
            appendAssetReservationConfirmedBatch(commands);
            return;
        }
        consumerTransactionTemplate.executeWithoutResult(status -> {
            for (OrderEventAppendCommand command : commands) {
                appendInTransaction(command, consumerJdbc);
            }
        });
    }

    private boolean isAssetReservationConfirmedBatch(List<OrderEventAppendCommand> commands) {
        Set<UUID> aggregateIds = new HashSet<>(commands.size());
        for (OrderEventAppendCommand command : commands) {
            if (command.expectedVersion() != 1
                    || command.integrationEvent() != null
                    || !(command.payload() instanceof OrderAssetReservationConfirmedV1)
                    || !"OrderAssetReservationConfirmedV1".equals(command.eventType())
                    || !aggregateIds.add(command.aggregateId())) {
                return false;
            }
        }
        return true;
    }

    private void appendAssetReservationConfirmedBatch(List<OrderEventAppendCommand> commands) {
        long startedNanos = System.nanoTime();
        long[] callbackStartedNanos = new long[1];
        long[] bodyCompletedNanos = new long[1];
        try {
            consumerTransactionTemplate.executeWithoutResult(status -> {
                callbackStartedNanos[0] = System.nanoTime();
                assetReservationAppendMetrics.recordNanos(
                        "transaction_before_callback",
                        callbackStartedNanos[0] - startedNanos);
                long bodyStartedNanos = System.nanoTime();
                try {
                    if (assetReservationConfirmedSingleRoundTripEnabled) {
                        boolean appended = appendAssetReservationConfirmedBatchSingleRoundTrip(commands);
                        if (!appended) {
                            long fallbackStartedNanos = System.nanoTime();
                            for (OrderEventAppendCommand command : commands) {
                                appendInTransaction(command, consumerJdbc);
                            }
                            assetReservationAppendMetrics.record(
                                    "fallback_individual_append",
                                    fallbackStartedNanos);
                        }
                        return;
                    }
                    long lockStartedNanos = System.nanoTime();
                    Map<UUID, StreamHead> heads = lockHeads(consumerJdbc, commands.stream()
                            .map(OrderEventAppendCommand::aggregateId)
                            .toList());
                    assetReservationAppendMetrics.record("lock_heads", lockStartedNanos);
                    if (heads.size() != commands.size()
                            || heads.values().stream().anyMatch(head -> head.currentVersion() != 1)) {
                        long fallbackStartedNanos = System.nanoTime();
                        for (OrderEventAppendCommand command : commands) {
                            appendInTransaction(command, consumerJdbc);
                        }
                        assetReservationAppendMetrics.record("fallback_individual_append", fallbackStartedNanos);
                        return;
                    }
                    appendAssetReservationConfirmedBatchWithLockedHeads(commands, heads);
                } finally {
                    bodyCompletedNanos[0] = System.nanoTime();
                    assetReservationAppendMetrics.recordNanos(
                            "transaction_body",
                            bodyCompletedNanos[0] - bodyStartedNanos);
                }
            });
        } finally {
            long completedNanos = System.nanoTime();
            assetReservationAppendMetrics.recordNanos("transaction_total", completedNanos - startedNanos);
            if (bodyCompletedNanos[0] > 0) {
                assetReservationAppendMetrics.recordNanos(
                        "transaction_after_body",
                        completedNanos - bodyCompletedNanos[0]);
            }
        }
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
            OrderTradeApplication tradeApplication) {
        long startedNanos = System.nanoTime();
        try {
            return consumerTransactionTemplate.execute(status ->
                    appendTradeMatchedFromCaughtUpProjectionIfTradeApplicationAbsentInTransaction(
                            buyerCommand, buyerMatchedQuantity,
                            sellerCommand, sellerMatchedQuantity,
                            tradeApplication));
        } finally {
            tradeApplyMetrics.record("total", startedNanos);
        }
    }

    public TradeApplicationBatchAppendResult appendTradeMatchedBatchFromCaughtUpProjectionIfTradeApplicationsAbsent(
            List<TradeApplicationBatchAppendCommand> commands) {
        if (commands == null || commands.isEmpty()) {
            return TradeApplicationBatchAppendResult.applied(0);
        }
        long startedNanos = System.nanoTime();
        try {
            return consumerTransactionTemplate.execute(status ->
                    appendTradeMatchedBatchFromCaughtUpProjectionIfTradeApplicationsAbsentInTransaction(
                            commands));
        } finally {
            tradeApplyMetrics.record("batch_total", startedNanos);
        }
    }

    private OrderEventAppendResult appendInTransaction(
            OrderEventAppendCommand command,
            NamedParameterJdbcTemplate jdbc) {
        return appendInTransaction(command, jdbc, false);
    }

    private OrderEventAppendResult appendInTransaction(
            OrderEventAppendCommand command,
            NamedParameterJdbcTemplate jdbc,
            boolean recordSubmissionAppend) {
        if (recordSubmissionAppend && command.integrationEvent() != null) {
            OrderEventAppendResult fastPathResult = tryAppendInitialSubmissionFastPath(command, jdbc);
            if (fastPathResult != null) {
                return fastPathResult;
            }
        }
        if (command.expectedVersion() == 0) {
            long createHeadStartedNanos = System.nanoTime();
            createHeadIfAbsent(jdbc, command.aggregateId());
            if (recordSubmissionAppend) {
                submissionAppendMetrics.record("create_head_if_absent", createHeadStartedNanos);
            }
        }
        long lockHeadStartedNanos = System.nanoTime();
        StreamHead head = lockHead(jdbc, command.aggregateId());
        if (recordSubmissionAppend) {
            submissionAppendMetrics.record("lock_head", lockHeadStartedNanos);
        }
        return appendInTransactionWithLockedHead(command, jdbc, head, recordSubmissionAppend);
    }

    private OrderEventAppendResult tryAppendInitialSubmissionFastPath(
            OrderEventAppendCommand command,
            NamedParameterJdbcTemplate jdbc) {
        if (command.expectedVersion() != 0 || !(command.payload() instanceof OrderSubmissionRequestedV1 requested)) {
            return null;
        }
        if (submissionWriteMode == SubmissionWriteMode.EVENT_STORE_INTAKE) {
            return appendInitialSubmissionEventStoreOnly(command);
        }
        long serializeStartedNanos = System.nanoTime();
        String payloadCanonical = serialize(command.payload());
        String metadataCanonical = serialize(command.metadata());
        String integrationPayload = command.integrationEvent().payload() == command.payload()
                ? payloadCanonical
                : serialize(command.integrationEvent().payload());
        submissionAppendMetrics.record("initial_append_serialize_payload_metadata", serializeStartedNanos);
        long hashStartedNanos = System.nanoTime();
        String hash = computeHash(
                command,
                1,
                payloadCanonical,
                metadataCanonical,
                GENESIS_HASH
        );
        submissionAppendMetrics.record("initial_append_compute_hash", hashStartedNanos);
        long startedNanos = System.nanoTime();
        InitialSubmissionAppendCounts result = jdbc.queryForObject("""
                WITH inserted_head AS (
                    INSERT INTO order_service.order_stream_heads
                        (aggregate_id, current_version, last_event_id, last_hash,
                         user_id, remaining_amount, status, updated_at)
                    VALUES (:aggregateId, 1, :eventId, :hash,
                            :userId, :remainingAmount, 'PENDING_ASSET_CHECK', CURRENT_TIMESTAMP)
                    ON CONFLICT (aggregate_id) DO NOTHING
                    RETURNING aggregate_id
                ),
                inserted_event AS (
                    INSERT INTO order_service.order_event_store
                        (event_id, aggregate_id, aggregate_type, aggregate_version,
                         event_type, payload_canonical, metadata_canonical, schema_version,
                         occurred_at, prev_hash, hash)
                    SELECT :eventId, :aggregateId, :aggregateType, 1,
                           :eventType, :payload, :metadata, :schemaVersion,
                           :occurredAt, :genesisHash, :hash
                    FROM inserted_head
                    RETURNING global_position
                ),
                inserted_outbox AS (
                    INSERT INTO order_service.order_event_outbox
                        (event_id, aggregate_id, exchange_name, routing_key, message_type, payload,
                         status, attempt_count, next_retry_at, created_at, updated_at)
                    SELECT :eventId, :aggregateId, :exchange, :routingKey, :messageType, :integrationPayload,
                           'PENDING', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    FROM inserted_event
                    RETURNING id
                )
                SELECT
                    (SELECT COUNT(*) FROM inserted_head) AS inserted_head,
                    (SELECT COUNT(*) FROM inserted_event) AS inserted_event,
                    (SELECT COALESCE(MAX(global_position), 0) FROM inserted_event) AS global_position,
                    (SELECT COUNT(*) FROM inserted_head) AS updated_head,
                    (SELECT COUNT(*) FROM inserted_outbox) AS inserted_outbox
                """, new MapSqlParameterSource()
                .addValue("aggregateId", command.aggregateId())
                .addValue("eventId", command.eventId())
                .addValue("aggregateType", AGGREGATE_TYPE)
                .addValue("eventType", command.eventType())
                .addValue("payload", payloadCanonical)
                .addValue("metadata", metadataCanonical)
                .addValue("schemaVersion", command.schemaVersion())
                .addValue("occurredAt", command.occurredAt())
                .addValue("genesisHash", GENESIS_HASH)
                .addValue("hash", hash)
                .addValue("userId", requested.userId())
                .addValue("remainingAmount", requested.amount())
                .addValue("exchange", command.integrationEvent().exchange())
                .addValue("routingKey", command.integrationEvent().routingKey())
                .addValue("messageType", command.integrationEvent().payload().getClass().getName())
                .addValue("integrationPayload", integrationPayload),
                (rs, rowNum) -> new InitialSubmissionAppendCounts(
                        rs.getInt("inserted_head"),
                        rs.getInt("inserted_event"),
                        rs.getLong("global_position"),
                        rs.getInt("updated_head"),
                        rs.getInt("inserted_outbox")));
        submissionAppendMetrics.record("initial_append_cte", startedNanos);
        if (result == null || result.insertedHead() == 0) {
            return null;
        }
        if (result.insertedEvent() != 1
                || result.updatedHead() != 1
                || result.insertedOutbox() != 1
                || result.globalPosition() <= 0) {
            throw new IllegalStateException("Initial order submission append fast path did not converge: "
                    + result);
        }
        return new OrderEventAppendResult(
                command.aggregateId(),
                command.eventId(),
                1,
                result.globalPosition(),
                hash,
                false
        );
    }

    private OrderEventAppendResult appendInitialSubmissionEventStoreOnly(OrderEventAppendCommand command) {
        long serializeStartedNanos = System.nanoTime();
        String payloadCanonical = serialize(command.payload());
        String metadataCanonical = serialize(command.metadata());
        submissionAppendMetrics.record("event_store_intake_serialize_payload_metadata", serializeStartedNanos);
        long hashStartedNanos = System.nanoTime();
        String hash = computeHash(
                command,
                1,
                payloadCanonical,
                metadataCanonical,
                GENESIS_HASH
        );
        submissionAppendMetrics.record("event_store_intake_compute_hash", hashStartedNanos);
        long startedNanos = System.nanoTime();
        EventStoreOnlyAppendResult result = commandJdbc.queryForObject("""
                WITH inserted_event AS (
                    INSERT INTO order_service.order_event_store
                        (event_id, aggregate_id, aggregate_type, aggregate_version,
                         event_type, payload_canonical, metadata_canonical, schema_version,
                         occurred_at, prev_hash, hash)
                    VALUES (:eventId, :aggregateId, :aggregateType, 1,
                            :eventType, :payload, :metadata, :schemaVersion,
                            :occurredAt, :genesisHash, :hash)
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING global_position
                ),
                existing_event AS (
                    SELECT global_position
                    FROM order_service.order_event_store
                    WHERE event_id = :eventId
                      AND aggregate_id = :aggregateId
                      AND aggregate_version = 1
                      AND event_type = :eventType
                )
                SELECT
                    (SELECT COUNT(*) FROM inserted_event) AS inserted_event,
                    (SELECT COALESCE(MAX(global_position), 0) FROM inserted_event) AS inserted_global_position,
                    (SELECT COALESCE(MAX(global_position), 0) FROM existing_event) AS existing_global_position
                """, new MapSqlParameterSource()
                .addValue("eventId", command.eventId())
                .addValue("aggregateId", command.aggregateId())
                .addValue("aggregateType", AGGREGATE_TYPE)
                .addValue("eventType", command.eventType())
                .addValue("payload", payloadCanonical)
                .addValue("metadata", metadataCanonical)
                .addValue("schemaVersion", command.schemaVersion())
                .addValue("occurredAt", command.occurredAt())
                .addValue("genesisHash", GENESIS_HASH)
                .addValue("hash", hash),
                (rs, rowNum) -> new EventStoreOnlyAppendResult(
                        rs.getInt("inserted_event"),
                        rs.getLong("inserted_global_position"),
                        rs.getLong("existing_global_position")));
        submissionAppendMetrics.record("event_store_intake_insert_event", startedNanos);
        if (result == null || result.globalPosition() <= 0) {
            throw new IllegalStateException("Order submission event-store intake did not converge: eventId="
                    + command.eventId());
        }
        return new OrderEventAppendResult(
                command.aggregateId(),
                command.eventId(),
                1,
                result.globalPosition(),
                hash,
                result.insertedEvent() == 0
        );
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
            state = lockStreamHeadAsMatchingState(commandJdbc, orderId);
        }
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
            OrderTradeApplication tradeApplication) {
        validateDistinctTradeOrders(buyerDraftCommand, sellerDraftCommand);
        long lockStartedNanos = System.nanoTime();
        Map<UUID, MatchingState> states = lockMatchingStatesInStableOrder(
                buyerDraftCommand.aggregateId(), sellerDraftCommand.aggregateId());
        tradeApplyMetrics.record("lock_heads", lockStartedNanos);
        MatchingState buyerState = states.get(buyerDraftCommand.aggregateId());
        MatchingState sellerState = states.get(sellerDraftCommand.aggregateId());
        if (buyerState == null || sellerState == null) {
            if (existingTradeApplicationMatches(tradeApplication)) {
                return TradeExecutionAppendResult.duplicate();
            }
            return TradeExecutionAppendResult.missingPrerequisite();
        }
        if (!buyerState.canMatch(buyerMatchedQuantity)
                || !sellerState.canMatch(sellerMatchedQuantity)) {
            if (existingTradeApplicationMatches(tradeApplication)) {
                return TradeExecutionAppendResult.duplicate();
            }
            return TradeExecutionAppendResult.invalidOrderState();
        }
        long appendStartedNanos = System.nanoTime();
        TradeApplicationHotPathOutcome outcome = insertTradeApplicationAndMatchingState(
                buyerDraftCommand.aggregateId(),
                buyerMatchedQuantity,
                buyerState,
                sellerDraftCommand.aggregateId(),
                sellerMatchedQuantity,
                sellerState,
                tradeApplication);
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
            List<TradeApplicationBatchAppendCommand> commands) {
        Set<UUID> aggregateIds = new HashSet<>(commands.size() * 2);
        for (TradeApplicationBatchAppendCommand command : commands) {
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
        List<TradeApplicationHotPathBatchAppend> preparedBatch = new ArrayList<>(commands.size());
        for (TradeApplicationBatchAppendCommand command : commands) {
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
                    command.tradeApplication()));
        }
        tradeApplyMetrics.record("batch_prepare_append", prepareStartedNanos);

        long appendStartedNanos = System.nanoTime();
        TradeApplicationHotPathBatchOutcome outcome = insertTradeApplicationsAndMatchingStates(preparedBatch);
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
        return lockHeadsInStableOrder(List.of(first, second));
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
        return lockMatchingStatesInStableOrder(List.of(first, second));
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

    private MatchingState lockStreamHeadAsMatchingState(
            NamedParameterJdbcTemplate jdbc,
            UUID orderId) {
        List<MatchingState> states = jdbc.query("""
                SELECT user_id, remaining_amount, status
                FROM order_service.order_stream_heads
                WHERE aggregate_id = :orderId
                FOR UPDATE
                """, new MapSqlParameterSource("orderId", orderId),
                (rs, rowNum) -> new MatchingState(
                        rs.getObject("user_id", UUID.class),
                        (Integer) rs.getObject("remaining_amount") == null
                                ? 0
                                : rs.getInt("remaining_amount"),
                        0,
                        rs.getString("status")));
        return states.isEmpty() ? null : states.get(0);
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

    private TradeApplicationHotPathOutcome insertTradeApplicationAndMatchingState(
            UUID buyerOrderId,
            int buyerMatchedQuantity,
            MatchingState buyerState,
            UUID sellerOrderId,
            int sellerMatchedQuantity,
            MatchingState sellerState,
            OrderTradeApplication tradeApplication) {
        final int[] insertedTradeApplications = {0};
        final int[] updatedMatchingStates = {0};
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
                )
                SELECT
                    (SELECT COUNT(*) FROM trade_application) AS inserted_trade_applications,
                    (SELECT COUNT(*) FROM updated_matching_states) AS updated_matching_states
                """, tradeApplicationAppendParams(
                buyerOrderId,
                buyerMatchedQuantity,
                buyerState,
                sellerOrderId,
                sellerMatchedQuantity,
                sellerState,
                tradeApplication), rs -> {
            insertedTradeApplications[0] = rs.getInt("inserted_trade_applications");
            updatedMatchingStates[0] = rs.getInt("updated_matching_states");
        });
        return new TradeApplicationHotPathOutcome(
                insertedTradeApplications[0],
                updatedMatchingStates[0]);
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

    private TradeApplicationHotPathBatchOutcome insertTradeApplicationsAndMatchingStates(
            List<TradeApplicationHotPathBatchAppend> preparedBatch) {
        if (preparedBatch.isEmpty()) {
            return new TradeApplicationHotPathBatchOutcome(0, 0, 0);
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
            try (PreparedStatement statement = connection.prepareStatement(
                    INSERT_TRADE_APPLICATIONS_AND_MATCHING_STATES_SQL)) {
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

                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Order trade application batch did not return an outcome");
                    }
                    return new TradeApplicationHotPathBatchOutcome(
                            rs.getInt("existing_trade_applications"),
                            rs.getInt("inserted_trade_applications"),
                            rs.getInt("updated_matching_states"));
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

    private void assertBatchCount(String label, int expected, int actual) {
        if (actual != expected) {
            throw new IllegalStateException("Expected " + expected + " " + label
                    + " batch rows, actual=" + actual);
        }
    }

    private OrderEventAppendResult appendInTransactionWithLockedHead(
            OrderEventAppendCommand command,
            NamedParameterJdbcTemplate jdbc,
            StreamHead head) {
        return appendInTransactionWithLockedHead(command, jdbc, head, false);
    }

    private OrderEventAppendResult appendInTransactionWithLockedHead(
            OrderEventAppendCommand command,
            NamedParameterJdbcTemplate jdbc,
            StreamHead head,
            boolean recordSubmissionAppend) {
        long serializeStartedNanos = System.nanoTime();
        String payloadCanonical = serialize(command.payload());
        String metadataCanonical = serialize(command.metadata());
        if (recordSubmissionAppend) {
            submissionAppendMetrics.record("serialize_payload_metadata", serializeStartedNanos);
        }

        if (head.currentVersion() != command.expectedVersion()) {
            long findExistingStartedNanos = System.nanoTime();
            ExistingEvent existing = findByEventId(jdbc, command.eventId());
            if (recordSubmissionAppend) {
                submissionAppendMetrics.record("find_existing_event", findExistingStartedNanos);
            }
            if (existing != null) {
                return existingAppendResult(command, existing, payloadCanonical, metadataCanonical);
            }
            throw new OrderEventVersionConflictException(
                    command.aggregateId(), command.expectedVersion(), head.currentVersion());
        }

        long nextVersion = head.currentVersion() + 1;
        long hashStartedNanos = System.nanoTime();
        String hash = computeHash(
                command,
                nextVersion,
                payloadCanonical,
                metadataCanonical,
                head.lastHash()
        );
        if (recordSubmissionAppend) {
            submissionAppendMetrics.record("compute_hash", hashStartedNanos);
        }
        long globalPosition;
        try {
            long insertEventStartedNanos = System.nanoTime();
            globalPosition = insertEvent(
                    jdbc,
                    command,
                    nextVersion,
                    payloadCanonical,
                    metadataCanonical,
                    head.lastHash(),
                    hash
            );
            if (recordSubmissionAppend) {
                submissionAppendMetrics.record("insert_event", insertEventStartedNanos);
            }
        } catch (DuplicateKeyException e) {
            long findExistingStartedNanos = System.nanoTime();
            ExistingEvent existing = findByEventId(jdbc, command.eventId());
            if (recordSubmissionAppend) {
                submissionAppendMetrics.record("find_existing_event", findExistingStartedNanos);
            }
            if (existing != null) {
                return existingAppendResult(command, existing, payloadCanonical, metadataCanonical);
            }
            throw e;
        }
        long updateHeadStartedNanos = System.nanoTime();
        updateHead(jdbc, command, head, nextVersion, hash);
        if (recordSubmissionAppend) {
            submissionAppendMetrics.record("update_head_and_matching_state", updateHeadStartedNanos);
        }
        long insertOutboxStartedNanos = System.nanoTime();
        insertOutboxIfPresent(jdbc, command, payloadCanonical);
        if (recordSubmissionAppend) {
            submissionAppendMetrics.record("insert_outbox", insertOutboxStartedNanos);
        }

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

    private Map<UUID, StreamHead> lockHeads(NamedParameterJdbcTemplate jdbc, List<UUID> aggregateIds) {
        if (aggregateIds.isEmpty()) {
            return Map.of();
        }
        Map<UUID, StreamHead> heads = new HashMap<>(aggregateIds.size());
        jdbc.query("""
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

    private boolean appendAssetReservationConfirmedBatchSingleRoundTrip(
            List<OrderEventAppendCommand> commands) {
        long callbackStartedNanos = System.nanoTime();
        Boolean appended = consumerJdbc.getJdbcTemplate().execute((ConnectionCallback<Boolean>) connection -> {
            Array eventIds = null;
            Array aggregateIds = null;
            Array eventTypes = null;
            Array payloads = null;
            Array metadatas = null;
            Array schemaVersions = null;
            Array occurredAts = null;
            Array hashMaterialPrefixes = null;
            Array currentVersions = null;
            Array newVersions = null;
            Array userIds = null;
            Array statuses = null;
            long statementStartedNanos = System.nanoTime();
            try (PreparedStatement statement =
                         connection.prepareStatement(APPEND_ASSET_RESERVATION_CONFIRMED_SINGLE_ROUND_TRIP_SQL)) {
                assetReservationAppendMetrics.record("prepare_statement", statementStartedNanos);
                long prepareArraysStartedNanos = System.nanoTime();
                AssetReservationConfirmedSingleRoundTripBatchArrays arrays =
                        assetReservationConfirmedSingleRoundTripBatchArrays(commands);
                assetReservationAppendMetrics.record("prepare_batch_arrays", prepareArraysStartedNanos);
                long createSqlArraysStartedNanos = System.nanoTime();
                eventIds = connection.createArrayOf("uuid", arrays.eventIds());
                aggregateIds = connection.createArrayOf("uuid", arrays.aggregateIds());
                eventTypes = connection.createArrayOf("varchar", arrays.eventTypes());
                payloads = connection.createArrayOf("text", arrays.payloads());
                metadatas = connection.createArrayOf("text", arrays.metadatas());
                schemaVersions = connection.createArrayOf("integer", arrays.schemaVersions());
                occurredAts = connection.createArrayOf("timestamp", arrays.occurredAts());
                hashMaterialPrefixes = connection.createArrayOf("text", arrays.hashMaterialPrefixes());
                currentVersions = connection.createArrayOf("bigint", arrays.currentVersions());
                newVersions = connection.createArrayOf("bigint", arrays.newVersions());
                userIds = connection.createArrayOf("uuid", arrays.userIds());
                statuses = connection.createArrayOf("varchar", arrays.statuses());
                assetReservationAppendMetrics.record("create_sql_arrays", createSqlArraysStartedNanos);
                long bindArraysStartedNanos = System.nanoTime();
                statement.setArray(1, eventIds);
                statement.setArray(2, aggregateIds);
                statement.setArray(3, eventTypes);
                statement.setArray(4, payloads);
                statement.setArray(5, metadatas);
                statement.setArray(6, schemaVersions);
                statement.setArray(7, occurredAts);
                statement.setArray(8, hashMaterialPrefixes);
                statement.setArray(9, currentVersions);
                statement.setArray(10, newVersions);
                statement.setArray(11, userIds);
                statement.setArray(12, statuses);
                assetReservationAppendMetrics.record("bind_sql_arrays", bindArraysStartedNanos);
                long executeStartedNanos = System.nanoTime();
                try (ResultSet resultSet = statement.executeQuery()) {
                    assetReservationAppendMetrics.record("execute_cte", executeStartedNanos);
                    if (!resultSet.next()) {
                        throw new IllegalStateException(
                                "Asset reservation single-round-trip batch append did not return counts");
                    }
                    if (!resultSet.getBoolean("eligible")) {
                        assertBatchCount(
                                "ineligible asset reservation event insert",
                                0,
                                resultSet.getInt("inserted_events"));
                        assertBatchCount(
                                "ineligible asset reservation head update",
                                0,
                                resultSet.getInt("updated_heads"));
                        assertBatchCount(
                                "ineligible asset reservation matching-state upsert",
                                0,
                                resultSet.getInt("upserted_matching_state"));
                        return false;
                    }
                    assertBatchCount(
                            "asset reservation event insert",
                            commands.size(),
                            resultSet.getInt("inserted_events"));
                    assertBatchCount(
                            "asset reservation head update",
                            commands.size(),
                            resultSet.getInt("updated_heads"));
                    assertBatchCount(
                            "asset reservation matching-state upsert",
                            commands.size(),
                            resultSet.getInt("upserted_matching_state"));
                    return true;
                }
            } finally {
                freeQuietly(eventIds);
                freeQuietly(aggregateIds);
                freeQuietly(eventTypes);
                freeQuietly(payloads);
                freeQuietly(metadatas);
                freeQuietly(schemaVersions);
                freeQuietly(occurredAts);
                freeQuietly(hashMaterialPrefixes);
                freeQuietly(currentVersions);
                freeQuietly(newVersions);
                freeQuietly(userIds);
                freeQuietly(statuses);
                assetReservationAppendMetrics.record("connection_callback", callbackStartedNanos);
            }
        });
        return Boolean.TRUE.equals(appended);
    }

    private void appendAssetReservationConfirmedBatchWithLockedHeads(
            List<OrderEventAppendCommand> commands,
            Map<UUID, StreamHead> heads) {
        long callbackStartedNanos = System.nanoTime();
        consumerJdbc.getJdbcTemplate().execute((ConnectionCallback<Void>) connection -> {
            Array eventIds = null;
            Array aggregateIds = null;
            Array eventTypes = null;
            Array payloads = null;
            Array metadatas = null;
            Array schemaVersions = null;
            Array occurredAts = null;
            Array prevHashes = null;
            Array hashes = null;
            Array currentVersions = null;
            Array newVersions = null;
            Array userIds = null;
            Array remainingAmounts = null;
            Array statuses = null;
            long statementStartedNanos = System.nanoTime();
            try (PreparedStatement statement = connection.prepareStatement("""
                    WITH input(event_id, aggregate_id, event_type, payload_canonical, metadata_canonical,
                               schema_version, occurred_at, prev_hash, hash, current_version, new_version,
                               user_id, remaining_amount, order_status) AS (
                        SELECT *
                        FROM unnest(?::uuid[], ?::uuid[], ?::varchar[], ?::text[], ?::text[],
                                    ?::integer[], ?::timestamp[], ?::varchar[], ?::varchar[],
                                    ?::bigint[], ?::bigint[], ?::uuid[], ?::integer[], ?::varchar[])
                    ),
                    inserted_events AS (
                        INSERT INTO order_service.order_event_store
                            (event_id, aggregate_id, aggregate_type, aggregate_version,
                             event_type, payload_canonical, metadata_canonical, schema_version,
                             occurred_at, prev_hash, hash)
                        SELECT event_id, aggregate_id, 'Order', new_version,
                               event_type, payload_canonical, metadata_canonical, schema_version,
                               occurred_at, prev_hash, hash
                        FROM input
                        ON CONFLICT (event_id) DO NOTHING
                        RETURNING aggregate_id
                    ),
                    updated_heads AS (
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
                          AND head.current_version = input.current_version
                        RETURNING head.aggregate_id
                    ),
                    upserted_matching_state AS (
                        INSERT INTO order_service.order_matching_state
                            (order_id, user_id, remaining_amount, matched_amount, status, updated_at)
                        SELECT aggregate_id, user_id, remaining_amount, 0, order_status, CURRENT_TIMESTAMP
                        FROM input
                        ON CONFLICT (order_id) DO UPDATE
                        SET user_id = EXCLUDED.user_id,
                            remaining_amount = EXCLUDED.remaining_amount,
                            status = EXCLUDED.status,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING order_id
                    )
                    SELECT
                        (SELECT COUNT(*) FROM inserted_events) AS inserted_events,
                        (SELECT COUNT(*) FROM updated_heads) AS updated_heads,
                        (SELECT COUNT(*) FROM upserted_matching_state) AS upserted_matching_state
                    """)) {
                assetReservationAppendMetrics.record("prepare_statement", statementStartedNanos);
                long prepareArraysStartedNanos = System.nanoTime();
                AssetReservationConfirmedBatchArrays arrays =
                        assetReservationConfirmedBatchArrays(commands, heads);
                assetReservationAppendMetrics.record("prepare_batch_arrays", prepareArraysStartedNanos);
                long createSqlArraysStartedNanos = System.nanoTime();
                eventIds = connection.createArrayOf("uuid", arrays.eventIds());
                aggregateIds = connection.createArrayOf("uuid", arrays.aggregateIds());
                eventTypes = connection.createArrayOf("varchar", arrays.eventTypes());
                payloads = connection.createArrayOf("text", arrays.payloads());
                metadatas = connection.createArrayOf("text", arrays.metadatas());
                schemaVersions = connection.createArrayOf("integer", arrays.schemaVersions());
                occurredAts = connection.createArrayOf("timestamp", arrays.occurredAts());
                prevHashes = connection.createArrayOf("varchar", arrays.prevHashes());
                hashes = connection.createArrayOf("varchar", arrays.hashes());
                currentVersions = connection.createArrayOf("bigint", arrays.currentVersions());
                newVersions = connection.createArrayOf("bigint", arrays.newVersions());
                userIds = connection.createArrayOf("uuid", arrays.userIds());
                remainingAmounts = connection.createArrayOf("integer", arrays.remainingAmounts());
                statuses = connection.createArrayOf("varchar", arrays.statuses());
                assetReservationAppendMetrics.record("create_sql_arrays", createSqlArraysStartedNanos);
                long bindArraysStartedNanos = System.nanoTime();
                statement.setArray(1, eventIds);
                statement.setArray(2, aggregateIds);
                statement.setArray(3, eventTypes);
                statement.setArray(4, payloads);
                statement.setArray(5, metadatas);
                statement.setArray(6, schemaVersions);
                statement.setArray(7, occurredAts);
                statement.setArray(8, prevHashes);
                statement.setArray(9, hashes);
                statement.setArray(10, currentVersions);
                statement.setArray(11, newVersions);
                statement.setArray(12, userIds);
                statement.setArray(13, remainingAmounts);
                statement.setArray(14, statuses);
                assetReservationAppendMetrics.record("bind_sql_arrays", bindArraysStartedNanos);
                long executeStartedNanos = System.nanoTime();
                try (ResultSet resultSet = statement.executeQuery()) {
                    assetReservationAppendMetrics.record("execute_cte", executeStartedNanos);
                    if (!resultSet.next()) {
                        throw new IllegalStateException("Asset reservation batch append did not return counts");
                    }
                    assertBatchCount("asset reservation event insert", commands.size(),
                            resultSet.getInt("inserted_events"));
                    assertBatchCount("asset reservation head update", commands.size(),
                            resultSet.getInt("updated_heads"));
                    assertBatchCount("asset reservation matching-state upsert", commands.size(),
                            resultSet.getInt("upserted_matching_state"));
                }
                return null;
            } finally {
                freeQuietly(eventIds);
                freeQuietly(aggregateIds);
                freeQuietly(eventTypes);
                freeQuietly(payloads);
                freeQuietly(metadatas);
                freeQuietly(schemaVersions);
                freeQuietly(occurredAts);
                freeQuietly(prevHashes);
                freeQuietly(hashes);
                freeQuietly(currentVersions);
                freeQuietly(newVersions);
                freeQuietly(userIds);
                freeQuietly(remainingAmounts);
                freeQuietly(statuses);
                assetReservationAppendMetrics.record("connection_callback", callbackStartedNanos);
            }
        });
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
                sellerStatuses);
    }

    private AssetReservationConfirmedSingleRoundTripBatchArrays
            assetReservationConfirmedSingleRoundTripBatchArrays(
                    List<OrderEventAppendCommand> commands) {
        int size = commands.size();
        UUID[] eventIds = new UUID[size];
        UUID[] aggregateIds = new UUID[size];
        String[] eventTypes = new String[size];
        String[] payloads = new String[size];
        String[] metadatas = new String[size];
        Integer[] schemaVersions = new Integer[size];
        Timestamp[] occurredAts = new Timestamp[size];
        String[] hashMaterialPrefixes = new String[size];
        Long[] currentVersions = new Long[size];
        Long[] newVersions = new Long[size];
        UUID[] userIds = new UUID[size];
        String[] statuses = new String[size];

        for (int i = 0; i < size; i++) {
            OrderEventAppendCommand command = commands.get(i);
            OrderAssetReservationConfirmedV1 confirmed =
                    (OrderAssetReservationConfirmedV1) command.payload();
            String payloadCanonical = serialize(command.payload());
            String metadataCanonical = serialize(command.metadata());
            long nextVersion = command.expectedVersion() + 1;

            eventIds[i] = command.eventId();
            aggregateIds[i] = command.aggregateId();
            eventTypes[i] = command.eventType();
            payloads[i] = payloadCanonical;
            metadatas[i] = metadataCanonical;
            schemaVersions[i] = command.schemaVersion();
            occurredAts[i] = Timestamp.valueOf(command.occurredAt());
            hashMaterialPrefixes[i] = hashMaterialPrefix(
                    command,
                    nextVersion,
                    payloadCanonical,
                    metadataCanonical);
            currentVersions[i] = command.expectedVersion();
            newVersions[i] = nextVersion;
            userIds[i] = confirmed.userId();
            statuses[i] = "OPEN";
        }

        return new AssetReservationConfirmedSingleRoundTripBatchArrays(
                eventIds,
                aggregateIds,
                eventTypes,
                payloads,
                metadatas,
                schemaVersions,
                occurredAts,
                hashMaterialPrefixes,
                currentVersions,
                newVersions,
                userIds,
                statuses);
    }

    private AssetReservationConfirmedBatchArrays assetReservationConfirmedBatchArrays(
            List<OrderEventAppendCommand> commands,
            Map<UUID, StreamHead> heads) {
        int size = commands.size();
        UUID[] eventIds = new UUID[size];
        UUID[] aggregateIds = new UUID[size];
        String[] eventTypes = new String[size];
        String[] payloads = new String[size];
        String[] metadatas = new String[size];
        Integer[] schemaVersions = new Integer[size];
        Timestamp[] occurredAts = new Timestamp[size];
        String[] prevHashes = new String[size];
        String[] hashes = new String[size];
        Long[] currentVersions = new Long[size];
        Long[] newVersions = new Long[size];
        UUID[] userIds = new UUID[size];
        Integer[] remainingAmounts = new Integer[size];
        String[] statuses = new String[size];

        for (int i = 0; i < size; i++) {
            OrderEventAppendCommand command = commands.get(i);
            StreamHead head = heads.get(command.aggregateId());
            if (head == null) {
                throw new IllegalStateException("Order stream head not found for batch append: "
                        + command.aggregateId());
            }
            CommandState nextState = nextCommandState(head, command);
            String payloadCanonical = serialize(command.payload());
            String metadataCanonical = serialize(command.metadata());
            long nextVersion = head.currentVersion() + 1;
            String hash = computeHash(command, nextVersion, payloadCanonical, metadataCanonical, head.lastHash());

            eventIds[i] = command.eventId();
            aggregateIds[i] = command.aggregateId();
            eventTypes[i] = command.eventType();
            payloads[i] = payloadCanonical;
            metadatas[i] = metadataCanonical;
            schemaVersions[i] = command.schemaVersion();
            occurredAts[i] = Timestamp.valueOf(command.occurredAt());
            prevHashes[i] = head.lastHash();
            hashes[i] = hash;
            currentVersions[i] = head.currentVersion();
            newVersions[i] = nextVersion;
            userIds[i] = nextState.userId();
            remainingAmounts[i] = nextState.remainingAmount();
            statuses[i] = nextState.status();
        }

        return new AssetReservationConfirmedBatchArrays(
                eventIds,
                aggregateIds,
                eventTypes,
                payloads,
                metadatas,
                schemaVersions,
                occurredAts,
                prevHashes,
                hashes,
                currentVersions,
                newVersions,
                userIds,
                remainingAmounts,
                statuses);
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
        String material = hashMaterialPrefix(
                command,
                aggregateVersion,
                payloadCanonical,
                metadataCanonical) + prevHash;
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256")
                            .digest(material.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private String hashMaterialPrefix(
            OrderEventAppendCommand command,
            long aggregateVersion,
            String payloadCanonical,
            String metadataCanonical) {
        return command.eventId() + "|"
                + command.aggregateId() + "|"
                + aggregateVersion + "|"
                + command.eventType() + "|"
                + payloadCanonical + "|"
                + metadataCanonical + "|"
                + command.schemaVersion() + "|"
                + command.occurredAt() + "|";
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
            int updatedMatchingStates) {
    }

    private record TradeApplicationHotPathBatchOutcome(
            int existingTradeApplications,
            int insertedTradeApplications,
            int updatedMatchingStates) {
    }

    private record TradeApplicationHotPathBatchAppend(
            UUID buyerOrderId,
            int buyerMatchedQuantity,
            MatchingState buyerState,
            UUID sellerOrderId,
            int sellerMatchedQuantity,
            MatchingState sellerState,
            OrderTradeApplication tradeApplication) {
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
            String[] sellerStatuses) {
    }

    private record AssetReservationConfirmedSingleRoundTripBatchArrays(
            UUID[] eventIds,
            UUID[] aggregateIds,
            String[] eventTypes,
            String[] payloads,
            String[] metadatas,
            Integer[] schemaVersions,
            Timestamp[] occurredAts,
            String[] hashMaterialPrefixes,
            Long[] currentVersions,
            Long[] newVersions,
            UUID[] userIds,
            String[] statuses) {
    }

    private record AssetReservationConfirmedBatchArrays(
            UUID[] eventIds,
            UUID[] aggregateIds,
            String[] eventTypes,
            String[] payloads,
            String[] metadatas,
            Integer[] schemaVersions,
            Timestamp[] occurredAts,
            String[] prevHashes,
            String[] hashes,
            Long[] currentVersions,
            Long[] newVersions,
            UUID[] userIds,
            Integer[] remainingAmounts,
            String[] statuses) {
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
            OrderTradeApplication tradeApplication) {
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

        private static TradeExecutionAppendResult missingPrerequisite() {
            return new TradeExecutionAppendResult(TradeExecutionAppendStatus.MISSING_PREREQUISITE, null);
        }

        private static TradeExecutionAppendResult invalidOrderState() {
            return new TradeExecutionAppendResult(TradeExecutionAppendStatus.INVALID_ORDER_STATE, null);
        }
    }

    public enum TradeExecutionAppendStatus {
        APPLIED,
        DUPLICATE,
        MISSING_PREREQUISITE,
        INVALID_ORDER_STATE
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

    private record InitialSubmissionAppendCounts(
            int insertedHead,
            int insertedEvent,
            long globalPosition,
            int updatedHead,
            int insertedOutbox) {
    }

    private record EventStoreOnlyAppendResult(
            int insertedEvent,
            long insertedGlobalPosition,
            long existingGlobalPosition) {

        long globalPosition() {
            return insertedGlobalPosition > 0 ? insertedGlobalPosition : existingGlobalPosition;
        }
    }

    private enum SubmissionWriteMode {
        CURRENT_ORDER_PATH,
        EVENT_STORE_INTAKE;

        private static SubmissionWriteMode parse(String value) {
            if (value == null || value.isBlank()) {
                return CURRENT_ORDER_PATH;
            }
            return SubmissionWriteMode.valueOf(value.trim().toUpperCase());
        }
    }
}
