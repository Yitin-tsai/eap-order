package com.eap.eap_order.loadtest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OrderSubmissionDbCeilingProbe {

    private static final String DEFAULT_JDBC_URL = "jdbc:postgresql://localhost:15432/eap_order_db";
    private static final String DEFAULT_USERNAME = "admin";
    private static final String DEFAULT_PASSWORD = "admin123";
    private static final String GENESIS_HASH = "0".repeat(64);

    private static final String INITIAL_APPEND_SQL = """
            WITH inserted_head AS (
                INSERT INTO order_service.order_stream_heads
                    (aggregate_id, current_version, last_event_id, last_hash,
                     user_id, remaining_amount, status, updated_at)
                VALUES (?, 1, ?, ?,
                        ?, ?, 'PENDING_ASSET_CHECK', CURRENT_TIMESTAMP)
                ON CONFLICT (aggregate_id) DO NOTHING
                RETURNING aggregate_id
            ),
            inserted_event AS (
                INSERT INTO order_service.order_event_store
                    (event_id, aggregate_id, aggregate_type, aggregate_version,
                     event_type, payload_canonical, metadata_canonical, schema_version,
                     occurred_at, prev_hash, hash)
                SELECT ?, ?, 'Order', 1,
                       'OrderSubmissionRequestedV1', ?, ?, 1,
                       ?, ?, ?
                FROM inserted_head
                RETURNING global_position
            ),
            upserted_matching_state AS (
                INSERT INTO order_service.order_matching_state
                    (order_id, user_id, remaining_amount, matched_amount, status, updated_at)
                SELECT ?, ?, ?, 0, 'PENDING_ASSET_CHECK', CURRENT_TIMESTAMP
                FROM inserted_head
                ON CONFLICT (order_id) DO UPDATE
                SET user_id = EXCLUDED.user_id,
                    remaining_amount = EXCLUDED.remaining_amount,
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING order_id
            ),
            inserted_outbox AS (
                INSERT INTO order_service.order_event_outbox
                    (event_id, aggregate_id, exchange_name, routing_key, message_type, payload,
                     status, attempt_count, next_retry_at, created_at, updated_at)
                SELECT ?, ?, 'order.exchange', 'order.submitted', 'com.eap.common.event.OrderSubmittedEvent', ?,
                       'PENDING', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                FROM inserted_event
                RETURNING id
            )
            SELECT
                (SELECT COUNT(*) FROM inserted_head) AS inserted_head,
                (SELECT COUNT(*) FROM inserted_event) AS inserted_event,
                (SELECT COUNT(*) FROM inserted_head) AS updated_head,
                (SELECT COUNT(*) FROM upserted_matching_state) AS upserted_matching_state,
                (SELECT COUNT(*) FROM inserted_outbox) AS inserted_outbox
            """;

    public static void main(String[] args) throws Exception {
        Config config = Config.from(args);
        Result result = run(config);
        printJson(config, result);
        if (result.failures() > 0) {
            throw new IllegalStateException("Order submission DB ceiling probe failed rows=" + result.failures());
        }
    }

    private static Result run(Config config) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(config.workers());
        CountDownLatch done = new CountDownLatch(config.events());
        AtomicInteger next = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        List<Long> rowLatenciesNanos = Collections.synchronizedList(new ArrayList<>());

        long started = System.nanoTime();
        for (int worker = 0; worker < config.workers(); worker++) {
            executor.execute(() -> runWorker(config, next, done, failures, rowLatenciesNanos));
        }
        done.await();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        double elapsedSeconds = (System.nanoTime() - started) / 1_000_000_000.0;

        List<Long> sorted = new ArrayList<>(rowLatenciesNanos);
        Collections.sort(sorted);
        return new Result(
                config.events() - failures.get(),
                failures.get(),
                elapsedSeconds,
                percentileMillis(sorted, 0.50),
                percentileMillis(sorted, 0.95),
                percentileMillis(sorted, 0.99));
    }

    private static void runWorker(
            Config config,
            AtomicInteger next,
            CountDownLatch done,
            AtomicInteger failures,
            List<Long> rowLatenciesNanos) {
        try (Connection connection = DriverManager.getConnection(
                config.jdbcUrl(),
                config.username(),
                config.password());
             PreparedStatement statement = connection.prepareStatement(INITIAL_APPEND_SQL)) {
            connection.setAutoCommit(config.mode() == Mode.AUTOCOMMIT);
            while (true) {
                int index = next.getAndIncrement();
                if (index >= config.events()) {
                    return;
                }
                long rowStarted = System.nanoTime();
                try {
                    bindInitialAppend(statement, config.marketId(), index + 1, config.amount());
                    InitialAppendCounts counts = executeInitialAppend(statement);
                    if (!counts.completed()) {
                        throw new IllegalStateException("unexpected initial append counts=" + counts);
                    }
                    if (config.mode() != Mode.AUTOCOMMIT) {
                        connection.commit();
                    }
                    rowLatenciesNanos.add(System.nanoTime() - rowStarted);
                } catch (Exception e) {
                    failures.incrementAndGet();
                    rollbackQuietly(connection, config);
                    if (failures.get() <= 100) {
                        System.err.printf(
                                "order submission probe row failed: index=%d, error=%s%n",
                                index + 1,
                                e.getMessage());
                    }
                } finally {
                    done.countDown();
                }
            }
        } catch (Exception e) {
            int remaining;
            do {
                remaining = next.getAndIncrement();
                if (remaining < config.events()) {
                    failures.incrementAndGet();
                    done.countDown();
                }
            } while (remaining < config.events());
            System.err.printf("order submission probe worker failed: %s%n", e.getMessage());
        }
    }

    private static void bindInitialAppend(
            PreparedStatement statement,
            String marketId,
            int index,
            int amount) throws Exception {
        UUID aggregateId = deterministicUuid(marketId + ":order:" + index);
        UUID userId = deterministicUuid(marketId + ":user:" + index);
        UUID eventId = deterministicUuid(aggregateId + ":REQUESTED");
        LocalDateTime occurredAt = LocalDateTime.now();
        String payload = payload(aggregateId, userId, marketId, index, amount, occurredAt);
        String metadata = "{\"correlationId\":\"" + aggregateId + "\",\"userId\":\"" + userId + "\"}";
        String hash = sha256(aggregateId + "|" + eventId + "|" + payload + "|" + metadata);

        statement.setObject(1, aggregateId);
        statement.setObject(2, eventId);
        statement.setString(3, hash);
        statement.setObject(4, userId);
        statement.setInt(5, amount);
        statement.setObject(6, eventId);
        statement.setObject(7, aggregateId);
        statement.setString(8, payload);
        statement.setString(9, metadata);
        statement.setTimestamp(10, Timestamp.valueOf(occurredAt));
        statement.setString(11, GENESIS_HASH);
        statement.setString(12, hash);
        statement.setObject(13, aggregateId);
        statement.setObject(14, userId);
        statement.setInt(15, amount);
        statement.setObject(16, eventId);
        statement.setObject(17, aggregateId);
        statement.setString(18, payload);
    }

    private static String payload(
            UUID aggregateId,
            UUID userId,
            String marketId,
            int marketSequence,
            int amount,
            LocalDateTime occurredAt) {
        return "{"
                + "\"orderId\":\"" + aggregateId + "\","
                + "\"userId\":\"" + userId + "\","
                + "\"marketId\":\"" + marketId + "\","
                + "\"marketSequence\":" + marketSequence + ","
                + "\"side\":\"SELL\","
                + "\"price\":10,"
                + "\"amount\":" + amount + ","
                + "\"createdAt\":\"" + occurredAt + "\""
                + "}";
    }

    private static InitialAppendCounts executeInitialAppend(PreparedStatement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery()) {
            if (!rs.next()) {
                throw new IllegalStateException("initial append did not return counts");
            }
            return new InitialAppendCounts(
                    rs.getInt("inserted_head"),
                    rs.getInt("inserted_event"),
                    rs.getInt("updated_head"),
                    rs.getInt("upserted_matching_state"),
                    rs.getInt("inserted_outbox"));
        }
    }

    private static void rollbackQuietly(Connection connection, Config config) {
        if (config.mode() == Mode.AUTOCOMMIT) {
            return;
        }
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // ignore rollback failure in benchmark error path
        }
    }

    private static UUID deterministicUuid(String value) {
        return UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(String value) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static double percentileMillis(List<Long> sorted, double percentile) {
        if (sorted.isEmpty()) {
            return 0.0;
        }
        int index = (int) Math.ceil(percentile * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1))) / 1_000_000.0;
    }

    private static void printJson(Config config, Result result) {
        System.out.println("{");
        System.out.printf("  \"mode\": \"orderSubmissionDbCeilingProbe\",%n");
        System.out.printf("  \"transactionMode\": \"%s\",%n", config.mode().name().toLowerCase());
        System.out.printf("  \"marketId\": \"%s\",%n", config.marketId());
        System.out.printf("  \"events\": %d,%n", config.events());
        System.out.printf("  \"workers\": %d,%n", config.workers());
        System.out.printf("  \"amount\": %d,%n", config.amount());
        System.out.printf("  \"completed\": %d,%n", result.completed());
        System.out.printf("  \"failures\": %d,%n", result.failures());
        System.out.printf("  \"elapsedSeconds\": %.3f,%n", result.elapsedSeconds());
        System.out.printf("  \"orderSubmissionAppendTps\": %.2f,%n",
                result.completed() / Math.max(result.elapsedSeconds(), 0.001));
        System.out.printf("  \"rowP50Ms\": %.3f,%n", result.rowP50Ms());
        System.out.printf("  \"rowP95Ms\": %.3f,%n", result.rowP95Ms());
        System.out.printf("  \"rowP99Ms\": %.3f%n", result.rowP99Ms());
        System.out.println("}");
    }

    private enum Mode {
        AUTOCOMMIT,
        TRANSACTION_PER_ROW
    }

    private record InitialAppendCounts(
            int insertedHead,
            int insertedEvent,
            int updatedHead,
            int upsertedMatchingState,
            int insertedOutbox) {
        boolean completed() {
            return insertedHead == 1
                    && insertedEvent == 1
                    && updatedHead == 1
                    && upsertedMatchingState == 1
                    && insertedOutbox == 1;
        }
    }

    private record Result(
            int completed,
            int failures,
            double elapsedSeconds,
            double rowP50Ms,
            double rowP95Ms,
            double rowP99Ms) {
    }

    private record Config(
            String jdbcUrl,
            String username,
            String password,
            String marketId,
            int events,
            int workers,
            int amount,
            Mode mode) {

        private static Config from(String[] args) {
            return new Config(
                    stringArg(args, "--jdbc-url", DEFAULT_JDBC_URL),
                    stringArg(args, "--username", DEFAULT_USERNAME),
                    stringArg(args, "--password", DEFAULT_PASSWORD),
                    stringArg(args, "--market-id", "ORDER_SUBMISSION_DB_CEILING_" + UUID.randomUUID()),
                    intArg(args, "--events", 10_000),
                    intArg(args, "--workers", 35),
                    intArg(args, "--amount", 1),
                    parseMode(stringArg(args, "--mode", "transaction_per_row")));
        }

        private static Mode parseMode(String value) {
            return Mode.valueOf(value.toUpperCase());
        }

        private static int intArg(String[] args, String name, int defaultValue) {
            return Integer.parseInt(stringArg(args, name, String.valueOf(defaultValue)));
        }

        private static String stringArg(String[] args, String name, String defaultValue) {
            for (int i = 0; i < args.length - 1; i++) {
                if (name.equals(args[i])) {
                    return args[i + 1];
                }
            }
            return defaultValue;
        }
    }
}
