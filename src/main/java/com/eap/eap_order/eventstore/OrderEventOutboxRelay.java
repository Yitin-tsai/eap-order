package com.eap.eap_order.eventstore;

import com.eap.eap_order.configuration.publishing.OrderPublishMetrics;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class OrderEventOutboxRelay {

    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate namedJdbc;
    private final RabbitTemplate rabbitTemplate;
    private final OrderPublishMetrics metrics;
    private final int batchSize;
    private final int publishConcurrency;
    private final boolean batchConfirmEnabled;
    private final ExecutorService publishExecutor;
    private final long confirmTimeoutMs;

    public OrderEventOutboxRelay(
            @Qualifier("orderConsumerJdbcTemplate") JdbcTemplate jdbc,
            @Qualifier("orderConsumerNamedParameterJdbcTemplate") NamedParameterJdbcTemplate namedJdbc,
            RabbitTemplate rabbitTemplate,
            OrderPublishMetrics metrics,
            @Value("${eap.order-event-outbox.batch-size:200}") int batchSize,
            @Value("${eap.order-event-outbox.publish-concurrency:1}") int publishConcurrency,
            @Value("${eap.order-event-outbox.batch-confirm-enabled:false}") boolean batchConfirmEnabled,
            @Value("${eap.order-event-outbox.confirm-timeout-ms:5000}") long confirmTimeoutMs) {
        this.jdbc = jdbc;
        this.namedJdbc = namedJdbc;
        this.rabbitTemplate = rabbitTemplate;
        this.metrics = metrics;
        this.batchSize = batchSize;
        this.publishConcurrency = Math.max(1, publishConcurrency);
        this.batchConfirmEnabled = batchConfirmEnabled;
        this.publishExecutor = this.publishConcurrency > 1
                ? Executors.newFixedThreadPool(this.publishConcurrency, new OrderOutboxPublishThreadFactory())
                : null;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    @PreDestroy
    public void shutdown() {
        if (publishExecutor != null) {
            publishExecutor.shutdown();
        }
    }

    @Scheduled(fixedDelayString = "${eap.order-event-outbox.poll-interval-ms:100}")
    public void relay() {
        boolean continueDraining;
        do {
            Instant batchStartedAt = Instant.now();
            Instant selectStartedAt = Instant.now();
            List<OutboxRow> rows;
            try {
                rows = jdbc.query("""
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
            } finally {
                metrics.recordOutboxSelect(Duration.between(selectStartedAt, Instant.now()));
            }
            if (rows.isEmpty()) {
                return;
            }

            List<PublishAttempt> attempts = new ArrayList<>(rows.size());
            boolean batchSucceeded = true;
            List<PublishResult> publishResults = publishBatch(rows);
            for (PublishResult result : publishResults) {
                if (result.succeeded()) {
                    attempts.add(new PublishAttempt(result.row(), result.correlation(), result.startedAt()));
                } else {
                    batchSucceeded = false;
                    recordFailure(result.row(), result.failure());
                    metrics.failed();
                    metrics.recordDuration(Duration.between(result.startedAt(), Instant.now()));
                }
            }

            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(confirmTimeoutMs);
            List<PublishAttempt> confirmed = new ArrayList<>(attempts.size());
            if (batchConfirmEnabled) {
                confirmed.addAll(attempts);
            } else {
                for (PublishAttempt attempt : attempts) {
                    Instant confirmStartedAt = Instant.now();
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
                    } finally {
                        metrics.recordOutboxConfirm(Duration.between(confirmStartedAt, Instant.now()));
                    }
                }
            }

            if (!confirmed.isEmpty()) {
                List<Long> ids = confirmed.stream().map(a -> a.row().id()).toList();
                Instant markStartedAt = Instant.now();
                try {
                    namedJdbc.update("""
                            UPDATE order_service.order_event_outbox
                            SET status = 'SENT', published_at = CURRENT_TIMESTAMP,
                                updated_at = CURRENT_TIMESTAMP, last_error = NULL,
                                next_retry_at = NULL
                            WHERE id IN (:ids) AND status = 'PENDING'
                            """, new MapSqlParameterSource("ids", ids));
                } finally {
                    metrics.recordOutboxMarkSent(Duration.between(markStartedAt, Instant.now()));
                }
                Instant completed = Instant.now();
                for (PublishAttempt attempt : confirmed) {
                    metrics.confirmed();
                    metrics.recordDuration(Duration.between(attempt.startedAt(), completed));
                }
            }
            continueDraining = batchSucceeded && rows.size() == batchSize;
            metrics.recordOutboxBatch(Duration.between(batchStartedAt, Instant.now()));
        } while (continueDraining);
    }

    private List<PublishResult> publishBatch(List<OutboxRow> rows) {
        if (publishConcurrency == 1 || rows.size() <= 1) {
            return publishChunk(rows);
        }

        List<List<OutboxRow>> chunks = partition(rows, publishConcurrency);
        List<CompletableFuture<List<PublishResult>>> futures = chunks.stream()
                .map(chunk -> CompletableFuture.supplyAsync(() -> publishChunk(chunk), publishExecutor))
                .toList();
        return futures.stream()
                .map(CompletableFuture::join)
                .flatMap(List::stream)
                .toList();
    }

    private List<PublishResult> publishChunk(List<OutboxRow> chunk) {
        List<PublishResult> results = new ArrayList<>(chunk.size());
        try {
            rabbitTemplate.invoke(operations -> {
                for (OutboxRow row : chunk) {
                    results.add(publishOne(row, operations));
                }
                if (batchConfirmEnabled && !results.isEmpty()) {
                    Instant confirmStartedAt = Instant.now();
                    operations.waitForConfirmsOrDie(confirmTimeoutMs);
                    Duration confirmDuration = Duration.between(confirmStartedAt, Instant.now());
                    recordBatchConfirm(results.size(), confirmDuration);
                    for (PublishResult result : results) {
                        if (result.correlation().getReturned() != null) {
                            throw new AmqpException(
                                    "Unroutable Order integration event: " + result.row().eventId());
                        }
                    }
                }
                return null;
            });
        } catch (Exception e) {
            if (batchConfirmEnabled && !results.isEmpty()) {
                results.clear();
                for (OutboxRow row : chunk) {
                    results.add(PublishResult.failure(row, Instant.now(), e));
                }
                return results;
            }
            int publishedOrFailed = results.size();
            for (int i = publishedOrFailed; i < chunk.size(); i++) {
                results.add(PublishResult.failure(chunk.get(i), Instant.now(), e));
            }
        }
        return results;
    }

    private void recordBatchConfirm(int confirmedCount, Duration confirmDuration) {
        if (confirmedCount <= 0) {
            return;
        }
        Duration perMessageDuration = confirmDuration.dividedBy(confirmedCount);
        for (int i = 0; i < confirmedCount; i++) {
            metrics.recordOutboxConfirm(perMessageDuration);
        }
    }

    private List<List<OutboxRow>> partition(List<OutboxRow> rows, int maxChunks) {
        int chunkCount = Math.min(maxChunks, rows.size());
        int chunkSize = (int) Math.ceil(rows.size() / (double) chunkCount);
        List<List<OutboxRow>> chunks = new ArrayList<>(chunkCount);
        for (int start = 0; start < rows.size(); start += chunkSize) {
            chunks.add(rows.subList(start, Math.min(start + chunkSize, rows.size())));
        }
        return chunks;
    }

    private PublishResult publishOne(OutboxRow row, RabbitOperations operations) {
        Instant startedAt = Instant.now();
        Instant enqueueStartedAt = Instant.now();
        try {
            CorrelationData correlation = new CorrelationData(row.eventId().toString());
            operations.send(row.exchange(), row.routingKey(), toJsonMessage(row), correlation);
            return PublishResult.success(row, correlation, startedAt);
        } catch (Exception e) {
            return PublishResult.failure(row, startedAt, e);
        } finally {
            metrics.recordOutboxPublishEnqueue(Duration.between(enqueueStartedAt, Instant.now()));
        }
    }

    private Message toJsonMessage(OutboxRow row) {
        if (!"com.eap.common.event.OrderSubmittedEvent".equals(row.messageType())
                && !"com.eap.common.event.OrderTradeAppliedEvent".equals(row.messageType())) {
            throw new IllegalArgumentException("Unsupported Order outbox message type: " + row.messageType());
        }
        MessageProperties properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        properties.setContentEncoding(StandardCharsets.UTF_8.name());
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        return new Message(row.payload().getBytes(StandardCharsets.UTF_8), properties);
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

    private record PublishResult(
            OutboxRow row,
            CorrelationData correlation,
            Instant startedAt,
            Exception failure) {

        static PublishResult success(OutboxRow row, CorrelationData correlation, Instant startedAt) {
            return new PublishResult(row, correlation, startedAt, null);
        }

        static PublishResult failure(OutboxRow row, Instant startedAt, Exception failure) {
            return new PublishResult(row, null, startedAt, failure);
        }

        boolean succeeded() {
            return failure == null;
        }
    }

    private static class OrderOutboxPublishThreadFactory implements java.util.concurrent.ThreadFactory {
        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "order-outbox-publisher-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
