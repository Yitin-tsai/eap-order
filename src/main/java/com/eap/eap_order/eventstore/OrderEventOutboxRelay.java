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
    private final boolean asyncRelayEnabled;
    private final int asyncMaxInFlightBatches;
    private final ExecutorService asyncRelayExecutor;
    private final AtomicInteger asyncInFlightBatches = new AtomicInteger();
    private final long inFlightTimeoutSeconds;
    private final long confirmTimeoutMs;

    public OrderEventOutboxRelay(
            @Qualifier("orderConsumerJdbcTemplate") JdbcTemplate jdbc,
            @Qualifier("orderConsumerNamedParameterJdbcTemplate") NamedParameterJdbcTemplate namedJdbc,
            RabbitTemplate rabbitTemplate,
            OrderPublishMetrics metrics,
            @Value("${eap.order-event-outbox.batch-size:200}") int batchSize,
            @Value("${eap.order-event-outbox.publish-concurrency:1}") int publishConcurrency,
            @Value("${eap.order-event-outbox.batch-confirm-enabled:false}") boolean batchConfirmEnabled,
            @Value("${eap.order-event-outbox.async-relay-enabled:false}") boolean asyncRelayEnabled,
            @Value("${eap.order-event-outbox.async-max-in-flight-batches:4}") int asyncMaxInFlightBatches,
            @Value("${eap.order-event-outbox.in-flight-timeout-seconds:30}") long inFlightTimeoutSeconds,
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
        this.asyncRelayEnabled = asyncRelayEnabled;
        this.asyncMaxInFlightBatches = Math.max(1, asyncMaxInFlightBatches);
        this.asyncRelayExecutor = asyncRelayEnabled
                ? Executors.newFixedThreadPool(this.asyncMaxInFlightBatches, new OrderOutboxAsyncRelayThreadFactory())
                : null;
        this.inFlightTimeoutSeconds = Math.max(1, inFlightTimeoutSeconds);
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    @PreDestroy
    public void shutdown() {
        if (publishExecutor != null) {
            publishExecutor.shutdown();
        }
        if (asyncRelayExecutor != null) {
            asyncRelayExecutor.shutdown();
        }
    }

    @Scheduled(fixedDelayString = "${eap.order-event-outbox.poll-interval-ms:100}")
    public void relay() {
        if (asyncRelayEnabled) {
            relayAsync();
            return;
        }

        boolean continueDraining;
        do {
            Instant batchStartedAt = Instant.now();
            List<OutboxRow> rows = selectPendingBatch();
            if (rows.isEmpty()) {
                return;
            }
            boolean batchSucceeded = processBatch(rows, "PENDING", batchStartedAt);
            continueDraining = batchSucceeded && rows.size() == batchSize;
        } while (continueDraining);
    }

    private void relayAsync() {
        while (asyncInFlightBatches.get() < asyncMaxInFlightBatches) {
            Instant batchStartedAt = Instant.now();
            List<OutboxRow> rows = claimBatchForAsyncRelay();
            if (rows.isEmpty()) {
                return;
            }
            asyncInFlightBatches.incrementAndGet();
            asyncRelayExecutor.submit(() -> {
                try {
                    processBatch(rows, "IN_FLIGHT", batchStartedAt);
                } finally {
                    asyncInFlightBatches.decrementAndGet();
                }
            });
        }
    }

    private List<OutboxRow> selectPendingBatch() {
        Instant selectStartedAt = Instant.now();
        try {
            return jdbc.query("""
                    SELECT id, event_id, exchange_name, routing_key, message_type,
                           payload, attempt_count
                    FROM order_service.order_event_outbox
                    WHERE status = 'PENDING'
                      AND next_retry_at <= CURRENT_TIMESTAMP
                    ORDER BY created_at, id
                    LIMIT ?
                    """, this::mapOutboxRow, batchSize);
        } finally {
            metrics.recordOutboxSelect(Duration.between(selectStartedAt, Instant.now()));
        }
    }

    private List<OutboxRow> claimBatchForAsyncRelay() {
        Instant selectStartedAt = Instant.now();
        try {
            return namedJdbc.query("""
                    WITH candidate AS (
                        SELECT id
                        FROM order_service.order_event_outbox
                        WHERE (status = 'PENDING' AND next_retry_at <= CURRENT_TIMESTAMP)
                           OR (status = 'IN_FLIGHT'
                               AND updated_at <= CURRENT_TIMESTAMP - (:inFlightTimeoutSeconds * INTERVAL '1 second'))
                        ORDER BY created_at, id
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                    ),
                    claimed AS (
                        UPDATE order_service.order_event_outbox outbox
                        SET status = 'IN_FLIGHT',
                            updated_at = CURRENT_TIMESTAMP,
                            last_error = NULL
                        FROM candidate
                        WHERE outbox.id = candidate.id
                        RETURNING outbox.id, outbox.event_id, outbox.exchange_name, outbox.routing_key,
                                  outbox.message_type, outbox.payload, outbox.attempt_count,
                                  outbox.created_at
                    )
                    SELECT id, event_id, exchange_name, routing_key, message_type, payload, attempt_count
                    FROM claimed
                    ORDER BY created_at, id
                    """, new MapSqlParameterSource()
                    .addValue("limit", batchSize)
                    .addValue("inFlightTimeoutSeconds", inFlightTimeoutSeconds), this::mapOutboxRow);
        } finally {
            metrics.recordOutboxSelect(Duration.between(selectStartedAt, Instant.now()));
        }
    }

    private OutboxRow mapOutboxRow(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        return new OutboxRow(
                rs.getLong("id"),
                rs.getObject("event_id", java.util.UUID.class),
                rs.getString("exchange_name"),
                rs.getString("routing_key"),
                rs.getString("message_type"),
                rs.getString("payload"),
                rs.getInt("attempt_count"));
    }

    private boolean processBatch(List<OutboxRow> rows, String expectedStatus, Instant batchStartedAt) {
        metrics.recordOutboxBatchSize(rows.size());

        List<PublishAttempt> attempts = new ArrayList<>(rows.size());
        boolean batchSucceeded = true;
        Instant publishStageStartedAt = Instant.now();
        List<PublishResult> publishResults;
        try {
            publishResults = publishBatch(rows);
        } finally {
            metrics.recordOutboxPublishStage(Duration.between(publishStageStartedAt, Instant.now()));
        }
        for (PublishResult result : publishResults) {
            if (result.succeeded()) {
                attempts.add(new PublishAttempt(result.row(), result.correlation(), result.startedAt()));
            } else {
                batchSucceeded = false;
                recordFailure(result.row(), result.failure(), expectedStatus);
                metrics.failed();
                metrics.recordDuration(Duration.between(result.startedAt(), Instant.now()));
            }
        }

        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(confirmTimeoutMs);
        List<PublishAttempt> confirmed = new ArrayList<>(attempts.size());
        Instant confirmStageStartedAt = Instant.now();
        if (batchConfirmEnabled) {
            confirmed.addAll(attempts);
        } else {
            boolean firstConfirm = true;
            for (PublishAttempt attempt : attempts) {
                Instant confirmStartedAt = Instant.now();
                Duration confirmDuration;
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
                    return false;
                } catch (Exception e) {
                    batchSucceeded = false;
                    recordFailure(attempt.row(), e, expectedStatus);
                    metrics.failed();
                    metrics.recordDuration(Duration.between(attempt.startedAt(), Instant.now()));
                } finally {
                    confirmDuration = Duration.between(confirmStartedAt, Instant.now());
                    metrics.recordOutboxConfirm(confirmDuration);
                    if (firstConfirm) {
                        metrics.recordOutboxFirstConfirm(confirmDuration);
                        firstConfirm = false;
                    } else {
                        metrics.recordOutboxRemainingConfirm(confirmDuration);
                    }
                }
            }
        }
        Instant confirmStageCompletedAt = Instant.now();
        if (!batchConfirmEnabled) {
            metrics.recordOutboxConfirmWall(Duration.between(confirmStageStartedAt, confirmStageCompletedAt));
        }

        if (!confirmed.isEmpty()) {
            markConfirmedAsSent(confirmed, expectedStatus, confirmStageCompletedAt);
        }
        metrics.recordOutboxBatch(Duration.between(batchStartedAt, Instant.now()));
        return batchSucceeded;
    }

    private void markConfirmedAsSent(
            List<PublishAttempt> confirmed,
            String expectedStatus,
            Instant confirmStageCompletedAt) {
        metrics.recordOutboxConfirmedBatchSize(confirmed.size());
        metrics.recordOutboxPostConfirmMarkGap(Duration.between(confirmStageCompletedAt, Instant.now()));
        List<Long> ids = confirmed.stream().map(a -> a.row().id()).toList();
        Instant markStartedAt = Instant.now();
        try {
            namedJdbc.update("""
                    UPDATE order_service.order_event_outbox
                    SET status = 'SENT', published_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP, last_error = NULL,
                        next_retry_at = NULL
                    WHERE id IN (:ids) AND status = :expectedStatus
                    """, new MapSqlParameterSource()
                    .addValue("ids", ids)
                    .addValue("expectedStatus", expectedStatus));
        } finally {
            metrics.recordOutboxMarkSent(Duration.between(markStartedAt, Instant.now()));
        }
        Instant completed = Instant.now();
        for (PublishAttempt attempt : confirmed) {
            metrics.confirmed();
            metrics.recordDuration(Duration.between(attempt.startedAt(), completed));
        }
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
                List<PublishResult> enqueued = results.stream()
                        .filter(PublishResult::succeeded)
                        .toList();
                if (batchConfirmEnabled && !enqueued.isEmpty()) {
                    Instant confirmStartedAt = Instant.now();
                    operations.waitForConfirmsOrDie(confirmTimeoutMs);
                    Duration confirmDuration = Duration.between(confirmStartedAt, Instant.now());
                    recordBatchConfirm(enqueued.size(), confirmDuration);
                    for (PublishResult result : enqueued) {
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
        metrics.recordOutboxConfirmWall(confirmDuration);
        metrics.recordOutboxFirstConfirm(confirmDuration);
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
                && !"com.eap.common.event.OrderCancellationRequestedEvent".equals(row.messageType())) {
            throw new IllegalArgumentException("Unsupported Order outbox message type: " + row.messageType());
        }
        MessageProperties properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        properties.setContentEncoding(StandardCharsets.UTF_8.name());
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        return new Message(row.payload().getBytes(StandardCharsets.UTF_8), properties);
    }

    private void recordFailure(OutboxRow row, Exception failure, String expectedStatus) {
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
                WHERE id = :id AND status = :expectedStatus
                """, new MapSqlParameterSource()
                .addValue("attempt", attempt)
                .addValue("backoffSeconds", backoffSeconds)
                .addValue("error", error.substring(0, Math.min(error.length(), 1000)))
                .addValue("expectedStatus", expectedStatus)
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

    private static class OrderOutboxAsyncRelayThreadFactory implements java.util.concurrent.ThreadFactory {
        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "order-outbox-async-relay-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
