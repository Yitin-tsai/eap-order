package com.eap.eap_order.eventstore;

import com.eap.common.event.OrderSubmittedEvent;
import com.eap.common.event.OrderTradeAppliedEvent;
import com.eap.eap_order.configuration.publishing.OrderPublishMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class OrderEventOutboxRelay {

    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate namedJdbc;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final OrderPublishMetrics metrics;
    private final int batchSize;
    private final long confirmTimeoutMs;

    public OrderEventOutboxRelay(
            @Qualifier("orderConsumerJdbcTemplate") JdbcTemplate jdbc,
            @Qualifier("orderConsumerNamedParameterJdbcTemplate") NamedParameterJdbcTemplate namedJdbc,
            RabbitTemplate rabbitTemplate,
            ObjectMapper objectMapper,
            OrderPublishMetrics metrics,
            @Value("${eap.order-event-outbox.batch-size:200}") int batchSize,
            @Value("${eap.order-event-outbox.confirm-timeout-ms:5000}") long confirmTimeoutMs) {
        this.jdbc = jdbc;
        this.namedJdbc = namedJdbc;
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.batchSize = batchSize;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    @Scheduled(fixedDelayString = "${eap.order-event-outbox.poll-interval-ms:100}")
    public void relay() {
        boolean continueDraining;
        do {
            List<OutboxRow> rows = jdbc.query("""
                    SELECT id, event_id, exchange_name, routing_key, message_type,
                           payload, attempt_count
                    FROM order_service.order_event_outbox
                    WHERE status = 'PENDING'
                      AND next_retry_at <= CURRENT_TIMESTAMP
                    ORDER BY created_at, id
                    LIMIT ?
                    """, (rs, rowNum) -> new OutboxRow(
                    rs.getLong("id"),
                    rs.getObject("event_id", java.util.UUID.class),
                    rs.getString("exchange_name"),
                    rs.getString("routing_key"),
                    rs.getString("message_type"),
                    rs.getString("payload"),
                    rs.getInt("attempt_count")), batchSize);
            if (rows.isEmpty()) {
                return;
            }

            List<PublishAttempt> attempts = new ArrayList<>(rows.size());
            boolean batchSucceeded = true;
            for (OutboxRow row : rows) {
                Instant startedAt = Instant.now();
                try {
                    Object message = deserialize(row);
                    CorrelationData correlation = new CorrelationData(row.eventId().toString());
                    rabbitTemplate.convertAndSend(
                            row.exchange(), row.routingKey(), message, correlation);
                    attempts.add(new PublishAttempt(row, correlation, startedAt));
                } catch (Exception e) {
                    batchSucceeded = false;
                    recordFailure(row, e);
                    metrics.failed();
                    metrics.recordDuration(Duration.between(startedAt, Instant.now()));
                }
            }

            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(confirmTimeoutMs);
            List<PublishAttempt> confirmed = new ArrayList<>(attempts.size());
            for (PublishAttempt attempt : attempts) {
                try {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        throw new TimeoutException("Order outbox confirm batch timed out");
                    }
                    CorrelationData.Confirm confirm = attempt.correlation().getFuture()
                            .get(remaining, TimeUnit.NANOSECONDS);
                    if (!confirm.isAck()) {
                        throw new AmqpException("RabbitMQ nack: " + confirm.getReason());
                    }
                    if (attempt.correlation().getReturned() != null) {
                        throw new AmqpException("Unroutable Order integration event: " + attempt.row().eventId());
                    }
                    confirmed.add(attempt);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    batchSucceeded = false;
                    recordFailure(attempt.row(), e);
                    metrics.failed();
                    metrics.recordDuration(Duration.between(attempt.startedAt(), Instant.now()));
                }
            }

            if (!confirmed.isEmpty()) {
                List<Long> ids = confirmed.stream().map(a -> a.row().id()).toList();
                namedJdbc.update("""
                        UPDATE order_service.order_event_outbox
                        SET status = 'SENT', published_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP, last_error = NULL,
                            next_retry_at = NULL
                        WHERE id IN (:ids) AND status = 'PENDING'
                        """, new MapSqlParameterSource("ids", ids));
                Instant completed = Instant.now();
                for (PublishAttempt attempt : confirmed) {
                    metrics.confirmed();
                    metrics.recordDuration(Duration.between(attempt.startedAt(), completed));
                }
            }
            continueDraining = batchSucceeded && rows.size() == batchSize;
        } while (continueDraining);
    }

    private Object deserialize(OutboxRow row) throws Exception {
        if (OrderSubmittedEvent.class.getName().equals(row.messageType())) {
            return objectMapper.readValue(row.payload(), OrderSubmittedEvent.class);
        }
        if (OrderTradeAppliedEvent.class.getName().equals(row.messageType())) {
            return objectMapper.readValue(row.payload(), OrderTradeAppliedEvent.class);
        }
        throw new IllegalArgumentException("Unsupported Order outbox message type: " + row.messageType());
    }

    private void recordFailure(OutboxRow row, Exception failure) {
        int attempt = row.attemptCount() + 1;
        String error = failure.getClass().getSimpleName() + ": " + failure.getMessage();
        long backoffSeconds = Math.min(300, 1L << Math.min(attempt - 1, 8));
        namedJdbc.update("""
                UPDATE order_service.order_event_outbox
                SET attempt_count = :attempt,
                    status = CASE WHEN :attempt >= 10 THEN 'FAILED' ELSE 'PENDING' END,
                    next_retry_at = CASE WHEN :attempt >= 10 THEN NULL
                        ELSE CURRENT_TIMESTAMP + (:backoffSeconds * INTERVAL '1 second') END,
                    last_error = :error,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
                """, new MapSqlParameterSource()
                .addValue("attempt", attempt)
                .addValue("backoffSeconds", backoffSeconds)
                .addValue("error", error.substring(0, Math.min(error.length(), 1000)))
                .addValue("id", row.id()));
    }

    private record OutboxRow(
            long id,
            java.util.UUID eventId,
            String exchange,
            String routingKey,
            String messageType,
            String payload,
            int attemptCount) {
    }

    private record PublishAttempt(OutboxRow row, CorrelationData correlation, Instant startedAt) {
    }
}
