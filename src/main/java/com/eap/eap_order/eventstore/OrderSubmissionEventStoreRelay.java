package com.eap.eap_order.eventstore;

import com.eap.common.event.OrderSubmittedEvent;
import com.eap.eap_order.configuration.publishing.OrderPublishMetrics;
import com.eap.eap_order.domain.ordersourcing.OrderSubmissionRequestedV1;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.eap.common.constants.RabbitMQConstants.ORDER_EXCHANGE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_SUBMITTED_KEY;

@Component
public class OrderSubmissionEventStoreRelay {

    private static final String RELAY_NAME = "order_submitted_event_store_relay";

    private final JdbcTemplate jdbc;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final OrderPublishMetrics metrics;
    private final TransactionTemplate transactionTemplate;
    private final boolean enabled;
    private final int batchSize;
    private final long confirmTimeoutMs;
    private final AtomicBoolean draining = new AtomicBoolean(false);

    public OrderSubmissionEventStoreRelay(
            @Qualifier("orderConsumerJdbcTemplate") JdbcTemplate jdbc,
            RabbitTemplate rabbitTemplate,
            ObjectMapper objectMapper,
            OrderPublishMetrics metrics,
            @Qualifier("orderConsumerTransactionManager") PlatformTransactionManager transactionManager,
            @Value("${eap.order.submission-event-store-relay.enabled:false}") boolean enabled,
            @Value("${eap.order.submission-event-store-relay.batch-size:500}") int batchSize,
            @Value("${eap.order.submission-event-store-relay.confirm-timeout-ms:5000}") long confirmTimeoutMs) {
        this.jdbc = jdbc;
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.enabled = enabled;
        this.batchSize = Math.max(1, batchSize);
        this.confirmTimeoutMs = Math.max(1, confirmTimeoutMs);
    }

    @Scheduled(fixedDelayString = "${eap.order.submission-event-store-relay.poll-interval-ms:25}")
    public void relay() {
        if (!enabled) {
            return;
        }
        if (!draining.compareAndSet(false, true)) {
            return;
        }
        try {
            boolean continueDraining;
            do {
                Instant batchStartedAt = Instant.now();
                List<RelayEvent> events = transactionTemplate.execute(status -> selectReadyBatch());
                if (events == null || events.isEmpty()) {
                    return;
                }
                publishAndCheckpoint(events, batchStartedAt);
                continueDraining = events.size() == batchSize;
            } while (continueDraining);
        } finally {
            draining.set(false);
        }
    }

    private List<RelayEvent> selectReadyBatch() {
        jdbc.update("""
                INSERT INTO order_service.order_event_store_relay_checkpoints
                    (relay_name, last_global_position, updated_at)
                VALUES (?, 0, CURRENT_TIMESTAMP)
                ON CONFLICT (relay_name) DO NOTHING
                """, RELAY_NAME);
        Long checkpoint = jdbc.queryForObject("""
                SELECT last_global_position
                FROM order_service.order_event_store_relay_checkpoints
                WHERE relay_name = ?
                FOR UPDATE
                """, Long.class, RELAY_NAME);
        List<RelayEvent> candidates = jdbc.query("""
                SELECT event_store.global_position,
                       event_store.event_id,
                       event_store.aggregate_id,
                       event_store.event_type,
                       event_store.payload_canonical,
                       COALESCE(head.current_version, 0) AS projected_version
                FROM order_service.order_event_store event_store
                LEFT JOIN order_service.order_stream_heads head
                  ON head.aggregate_id = event_store.aggregate_id
                WHERE event_store.global_position > ?
                ORDER BY event_store.global_position
                LIMIT ?
                """, (rs, rowNum) -> new RelayEvent(
                rs.getLong("global_position"),
                rs.getObject("event_id", UUID.class),
                rs.getObject("aggregate_id", UUID.class),
                rs.getString("event_type"),
                rs.getString("payload_canonical"),
                rs.getLong("projected_version")), checkpoint == null ? 0 : checkpoint, batchSize);
        candidates = contiguousPrefix(candidates, checkpoint == null ? 0 : checkpoint);
        if (candidates.stream().anyMatch(event -> event.isOrderSubmission() && event.projectedVersion() < 1)) {
            return List.of();
        }
        return candidates;
    }

    private List<RelayEvent> contiguousPrefix(List<RelayEvent> events, long checkpoint) {
        List<RelayEvent> prefix = new ArrayList<>(events.size());
        long expectedPosition = checkpoint + 1;
        for (RelayEvent event : events) {
            if (event.globalPosition() != expectedPosition) {
                break;
            }
            prefix.add(event);
            expectedPosition++;
        }
        return prefix;
    }

    private void publishAndCheckpoint(List<RelayEvent> events, Instant batchStartedAt) {
        metrics.recordOutboxBatchSize(events.size());
        List<RelayEvent> publishableEvents = events.stream()
                .filter(RelayEvent::isOrderSubmission)
                .toList();
        if (publishableEvents.isEmpty()) {
            checkpoint(events);
            return;
        }
        List<CorrelationData> correlations = new ArrayList<>(publishableEvents.size());
        Instant publishStageStartedAt = Instant.now();
        try {
            rabbitTemplate.invoke(operations -> {
                for (RelayEvent event : publishableEvents) {
                    Instant enqueueStartedAt = Instant.now();
                    try {
                        CorrelationData correlation = new CorrelationData(event.eventId().toString());
                        operations.send(ORDER_EXCHANGE, ORDER_SUBMITTED_KEY, toMessage(event), correlation);
                        correlations.add(correlation);
                    } finally {
                        metrics.recordOutboxPublishEnqueue(Duration.between(enqueueStartedAt, Instant.now()));
                    }
                }
                Instant confirmStartedAt = Instant.now();
                operations.waitForConfirmsOrDie(confirmTimeoutMs);
                Duration confirmDuration = Duration.between(confirmStartedAt, Instant.now());
                metrics.recordOutboxConfirmWall(confirmDuration);
                metrics.recordOutboxFirstConfirm(confirmDuration);
                for (CorrelationData correlation : correlations) {
                    if (correlation.getReturned() != null) {
                        throw new AmqpException("Unroutable Order submission event: "
                                + correlation.getId());
                    }
                    metrics.recordOutboxConfirm(confirmDuration.dividedBy(Math.max(1, correlations.size())));
                }
                return null;
            });
        } catch (Exception e) {
            metrics.failed();
            throw new IllegalStateException("Order submission event-store relay failed", e);
        } finally {
            metrics.recordOutboxPublishStage(Duration.between(publishStageStartedAt, Instant.now()));
        }

        checkpoint(events);
        metrics.recordOutboxConfirmedBatchSize(publishableEvents.size());
        Instant completed = Instant.now();
        for (RelayEvent event : publishableEvents) {
            metrics.confirmed();
            metrics.recordDuration(Duration.between(batchStartedAt, completed));
        }
        metrics.recordOutboxBatch(Duration.between(batchStartedAt, Instant.now()));
    }

    private void checkpoint(List<RelayEvent> events) {
        long lastPosition = events.get(events.size() - 1).globalPosition();
        Instant markStartedAt = Instant.now();
        jdbc.update("""
                UPDATE order_service.order_event_store_relay_checkpoints
                SET last_global_position = ?, updated_at = CURRENT_TIMESTAMP
                WHERE relay_name = ?
                """, lastPosition, RELAY_NAME);
        metrics.recordOutboxMarkSent(Duration.between(markStartedAt, Instant.now()));
    }

    private Message toMessage(RelayEvent event) {
        try {
            return toMessageOrThrow(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to build OrderSubmittedEvent from event_id="
                    + event.eventId(), e);
        }
    }

    private Message toMessageOrThrow(RelayEvent event) throws JsonProcessingException {
        OrderSubmissionRequestedV1 requested =
                objectMapper.readValue(event.payloadCanonical(), OrderSubmissionRequestedV1.class);
        OrderSubmittedEvent submitted = OrderSubmittedEvent.builder()
                .orderId(requested.orderId())
                .userId(requested.userId())
                .marketId(requested.marketId())
                .marketSequence(requested.marketSequence())
                .price(requested.price())
                .amount(requested.amount())
                .orderType(requested.side())
                .createdAt(requested.createdAt())
                .build();
        MessageProperties properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        properties.setContentEncoding(StandardCharsets.UTF_8.name());
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        return new Message(objectMapper.writeValueAsBytes(submitted), properties);
    }

    private record RelayEvent(
            long globalPosition,
            UUID eventId,
            UUID aggregateId,
            String eventType,
            String payloadCanonical,
            long projectedVersion) {
        boolean isOrderSubmission() {
            return "OrderSubmissionRequestedV1".equals(eventType);
        }
    }
}
