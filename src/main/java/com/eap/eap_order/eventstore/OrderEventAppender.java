package com.eap.eap_order.eventstore;

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

    private OrderEventAppendResult appendInTransaction(
            OrderEventAppendCommand command,
            NamedParameterJdbcTemplate jdbc) {
        String payloadCanonical = serialize(command.payload());
        String metadataCanonical = serialize(command.metadata());

        if (command.expectedVersion() == 0) {
            createHeadIfAbsent(jdbc, command.aggregateId());
        }
        StreamHead head = lockHead(jdbc, command.aggregateId());

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
        updateHead(jdbc, command.aggregateId(), head.currentVersion(), nextVersion, command.eventId(), hash);
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
                SELECT current_version, last_hash
                FROM order_service.order_stream_heads
                WHERE aggregate_id = :aggregateId
                FOR UPDATE
                """, Map.of("aggregateId", aggregateId),
                (rs, rowNum) -> new StreamHead(rs.getLong("current_version"), rs.getString("last_hash")));
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
            UUID aggregateId,
            long expectedVersion,
            long newVersion,
            UUID eventId,
            String hash) {
        int updated = jdbc.update("""
                UPDATE order_service.order_stream_heads
                SET current_version = :newVersion,
                    last_event_id = :eventId,
                    last_hash = :hash,
                    updated_at = CURRENT_TIMESTAMP
                WHERE aggregate_id = :aggregateId
                  AND current_version = :expectedVersion
                """, new MapSqlParameterSource()
                .addValue("newVersion", newVersion)
                .addValue("eventId", eventId)
                .addValue("hash", hash)
                .addValue("aggregateId", aggregateId)
                .addValue("expectedVersion", expectedVersion));
        if (updated != 1) {
            throw new OrderEventVersionConflictException(aggregateId, expectedVersion, newVersion);
        }
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

    private record StreamHead(long currentVersion, String lastHash) {
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
