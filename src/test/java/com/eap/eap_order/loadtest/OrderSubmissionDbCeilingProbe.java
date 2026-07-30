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

    private static final String CURRENT_ORDER_PATH_SQL = """
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
                (SELECT COUNT(*) FROM inserted_outbox) AS inserted_outbox
            """;

    private static final String EVENT_STORE_ONLY_SQL = """
            INSERT INTO order_service.order_submission_event_store_probe
                (run_id, event_id, aggregate_id, aggregate_type, aggregate_version,
                 event_type, payload_canonical, metadata_canonical, schema_version,
                 occurred_at, prev_hash, hash)
            VALUES
                (?, ?, ?, 'Order', 1, 'OrderSubmissionRequestedV1', ?, ?, 1, ?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
            """;

    private static final String INTAKE_LOG_ONLY_SQL = """
            INSERT INTO order_service.order_submission_intake_probe
                (run_id, command_id, order_id, user_id, market_id, market_sequence,
                 side, price, amount, payload, received_at)
            VALUES
                (?, ?, ?, ?, ?, ?, 'SELL', 10, ?, ?, ?)
            ON CONFLICT (command_id) DO NOTHING
            """;

    private static final String CREATE_EVENT_STORE_PROBE_SQL = """
            CREATE TABLE IF NOT EXISTS order_service.order_submission_event_store_probe (
                global_position BIGSERIAL PRIMARY KEY,
                run_id VARCHAR(200) NOT NULL,
                event_id UUID NOT NULL UNIQUE,
                aggregate_id UUID NOT NULL,
                aggregate_type VARCHAR(50) NOT NULL,
                aggregate_version BIGINT NOT NULL,
                event_type VARCHAR(100) NOT NULL,
                payload_canonical TEXT NOT NULL,
                metadata_canonical TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                occurred_at TIMESTAMP NOT NULL,
                prev_hash VARCHAR(64) NOT NULL,
                hash VARCHAR(64) NOT NULL
            )
            """;

    private static final String CREATE_EVENT_STORE_PROBE_AGGREGATE_INDEX_SQL = """
            CREATE UNIQUE INDEX IF NOT EXISTS uk_order_submission_event_store_probe_aggregate_version
                ON order_service.order_submission_event_store_probe(aggregate_id, aggregate_version)
            """;

    private static final String CREATE_EVENT_STORE_PROBE_RUN_INDEX_SQL = """
            CREATE INDEX IF NOT EXISTS idx_order_submission_event_store_probe_run_position
                ON order_service.order_submission_event_store_probe(run_id, global_position)
            """;

    private static final String CREATE_INTAKE_PROBE_SQL = """
            CREATE TABLE IF NOT EXISTS order_service.order_submission_intake_probe (
                id BIGSERIAL PRIMARY KEY,
                run_id VARCHAR(200) NOT NULL,
                command_id UUID NOT NULL UNIQUE,
                order_id UUID NOT NULL UNIQUE,
                user_id UUID NOT NULL,
                market_id VARCHAR(100) NOT NULL,
                market_sequence BIGINT NOT NULL,
                side VARCHAR(4) NOT NULL,
                price INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                payload TEXT NOT NULL,
                received_at TIMESTAMP NOT NULL
            )
            """;

    private static final String CREATE_INTAKE_PROBE_RUN_INDEX_SQL = """
            CREATE INDEX IF NOT EXISTS idx_order_submission_intake_probe_run_id
                ON order_service.order_submission_intake_probe(run_id, id)
            """;

    public static void main(String[] args) throws Exception {
        Config config = Config.from(args);
        prepareSchema(config);
        Result result = run(config);
        printJson(config, result);
        if (result.failures() > 0) {
            throw new IllegalStateException("Order submission DB ceiling probe failed rows=" + result.failures());
        }
    }

    private static void prepareSchema(Config config) throws SQLException {
        if (config.writePath() == WritePath.CURRENT_ORDER_PATH) {
            return;
        }
        try (Connection connection = DriverManager.getConnection(
                config.jdbcUrl(),
                config.username(),
                config.password());
             java.sql.Statement statement = connection.createStatement()) {
            if (config.writePath() == WritePath.EVENT_STORE_ONLY) {
                statement.execute(CREATE_EVENT_STORE_PROBE_SQL);
                statement.execute(CREATE_EVENT_STORE_PROBE_AGGREGATE_INDEX_SQL);
                statement.execute(CREATE_EVENT_STORE_PROBE_RUN_INDEX_SQL);
                if (config.cleanupProbeRows()) {
                    statement.executeUpdate("DELETE FROM order_service.order_submission_event_store_probe WHERE run_id = '"
                            + config.marketId().replace("'", "''") + "'");
                }
                return;
            }
            statement.execute(CREATE_INTAKE_PROBE_SQL);
            statement.execute(CREATE_INTAKE_PROBE_RUN_INDEX_SQL);
            if (config.cleanupProbeRows()) {
                statement.executeUpdate("DELETE FROM order_service.order_submission_intake_probe WHERE run_id = '"
                        + config.marketId().replace("'", "''") + "'");
            }
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
             PreparedStatement statement = connection.prepareStatement(sql(config.writePath()))) {
            connection.setAutoCommit(config.mode() == Mode.AUTOCOMMIT);
            while (true) {
                int index = next.getAndIncrement();
                if (index >= config.events()) {
                    return;
                }
                long rowStarted = System.nanoTime();
                try {
                    bind(statement, config, index + 1);
                    if (!execute(statement, config.writePath())) {
                        throw new IllegalStateException("unexpected append result for writePath="
                                + config.writePath());
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

    private static String sql(WritePath writePath) {
        return switch (writePath) {
            case CURRENT_ORDER_PATH -> CURRENT_ORDER_PATH_SQL;
            case EVENT_STORE_ONLY -> EVENT_STORE_ONLY_SQL;
            case INTAKE_LOG_ONLY -> INTAKE_LOG_ONLY_SQL;
        };
    }

    private static void bind(PreparedStatement statement, Config config, int index) throws Exception {
        SubmissionRow row = submissionRow(config.marketId(), index, config.amount());
        switch (config.writePath()) {
            case CURRENT_ORDER_PATH -> bindCurrentOrderPath(statement, row);
            case EVENT_STORE_ONLY -> bindEventStoreOnly(statement, config.marketId(), row);
            case INTAKE_LOG_ONLY -> bindIntakeLogOnly(statement, config.marketId(), index, row);
        }
    }

    private static SubmissionRow submissionRow(String marketId, int index, int amount) throws Exception {
        UUID aggregateId = deterministicUuid(marketId + ":order:" + index);
        UUID userId = deterministicUuid(marketId + ":user:" + index);
        UUID eventId = deterministicUuid(aggregateId + ":REQUESTED");
        LocalDateTime occurredAt = LocalDateTime.now();
        String payload = payload(aggregateId, userId, marketId, index, amount, occurredAt);
        String metadata = "{\"correlationId\":\"" + aggregateId + "\",\"userId\":\"" + userId + "\"}";
        String hash = sha256(aggregateId + "|" + eventId + "|" + payload + "|" + metadata);
        return new SubmissionRow(aggregateId, userId, eventId, occurredAt, amount, payload, metadata, hash);
    }

    private static void bindCurrentOrderPath(PreparedStatement statement, SubmissionRow row) throws SQLException {
        statement.setObject(1, row.aggregateId());
        statement.setObject(2, row.eventId());
        statement.setString(3, row.hash());
        statement.setObject(4, row.userId());
        statement.setInt(5, row.amount());
        statement.setObject(6, row.eventId());
        statement.setObject(7, row.aggregateId());
        statement.setString(8, row.payload());
        statement.setString(9, row.metadata());
        statement.setTimestamp(10, Timestamp.valueOf(row.occurredAt()));
        statement.setString(11, GENESIS_HASH);
        statement.setString(12, row.hash());
        statement.setObject(13, row.eventId());
        statement.setObject(14, row.aggregateId());
        statement.setString(15, row.payload());
    }

    private static void bindEventStoreOnly(PreparedStatement statement, String runId, SubmissionRow row)
            throws SQLException {
        statement.setString(1, runId);
        statement.setObject(2, row.eventId());
        statement.setObject(3, row.aggregateId());
        statement.setString(4, row.payload());
        statement.setString(5, row.metadata());
        statement.setTimestamp(6, Timestamp.valueOf(row.occurredAt()));
        statement.setString(7, GENESIS_HASH);
        statement.setString(8, row.hash());
    }

    private static void bindIntakeLogOnly(PreparedStatement statement, String runId, int index, SubmissionRow row)
            throws SQLException {
        statement.setString(1, runId);
        statement.setObject(2, row.eventId());
        statement.setObject(3, row.aggregateId());
        statement.setObject(4, row.userId());
        statement.setString(5, runId);
        statement.setLong(6, index);
        statement.setInt(7, row.amount());
        statement.setString(8, row.payload());
        statement.setTimestamp(9, Timestamp.valueOf(row.occurredAt()));
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

    private static boolean execute(PreparedStatement statement, WritePath writePath) throws SQLException {
        if (writePath != WritePath.CURRENT_ORDER_PATH) {
            return statement.executeUpdate() == 1;
        }
        try (ResultSet rs = statement.executeQuery()) {
            if (!rs.next()) {
                throw new IllegalStateException("initial append did not return counts");
            }
            CurrentOrderPathCounts counts = new CurrentOrderPathCounts(
                    rs.getInt("inserted_head"),
                    rs.getInt("inserted_event"),
                    rs.getInt("updated_head"),
                    rs.getInt("inserted_outbox"));
            return counts.completed();
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
        System.out.printf("  \"writePath\": \"%s\",%n", config.writePath().name().toLowerCase());
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

    private enum WritePath {
        CURRENT_ORDER_PATH,
        EVENT_STORE_ONLY,
        INTAKE_LOG_ONLY
    }

    private record CurrentOrderPathCounts(
            int insertedHead,
            int insertedEvent,
            int updatedHead,
            int insertedOutbox) {
        boolean completed() {
            return insertedHead == 1
                    && insertedEvent == 1
                    && updatedHead == 1
                    && insertedOutbox == 1;
        }
    }

    private record SubmissionRow(
            UUID aggregateId,
            UUID userId,
            UUID eventId,
            LocalDateTime occurredAt,
            int amount,
            String payload,
            String metadata,
            String hash) {
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
            Mode mode,
            WritePath writePath,
            boolean cleanupProbeRows) {

        private static Config from(String[] args) {
            return new Config(
                    stringArg(args, "--jdbc-url", DEFAULT_JDBC_URL),
                    stringArg(args, "--username", DEFAULT_USERNAME),
                    stringArg(args, "--password", DEFAULT_PASSWORD),
                    stringArg(args, "--market-id", "ORDER_SUBMISSION_DB_CEILING_" + UUID.randomUUID()),
                    intArg(args, "--events", 10_000),
                    intArg(args, "--workers", 35),
                    intArg(args, "--amount", 1),
                    parseMode(stringArg(args, "--mode", "transaction_per_row")),
                    parseWritePath(stringArg(args, "--write-path", "current_order_path")),
                    booleanArg(args, "--cleanup-probe-rows", true));
        }

        private static Mode parseMode(String value) {
            return Mode.valueOf(value.toUpperCase());
        }

        private static WritePath parseWritePath(String value) {
            return WritePath.valueOf(value.toUpperCase());
        }

        private static int intArg(String[] args, String name, int defaultValue) {
            return Integer.parseInt(stringArg(args, name, String.valueOf(defaultValue)));
        }

        private static boolean booleanArg(String[] args, String name, boolean defaultValue) {
            return Boolean.parseBoolean(stringArg(args, name, String.valueOf(defaultValue)));
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
