package com.eap.eap_order.loadtest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OrderDbCeilingProbe {

    private static final String DEFAULT_JDBC_URL = "jdbc:postgresql://localhost:15432/eap_order_db";
    private static final String DEFAULT_USERNAME = "admin";
    private static final String DEFAULT_PASSWORD = "admin123";

    private static final String SEED_MATCHING_STATE_SQL = """
            INSERT INTO order_service.order_matching_state
                (order_id, user_id, remaining_amount, matched_amount, status, updated_at)
            VALUES (?, ?, ?, 0, 'OPEN', CURRENT_TIMESTAMP)
            ON CONFLICT (order_id) DO UPDATE
            SET user_id = EXCLUDED.user_id,
                remaining_amount = EXCLUDED.remaining_amount,
                matched_amount = 0,
                status = 'OPEN',
                last_trade_id = NULL,
                updated_at = CURRENT_TIMESTAMP
            """;

    public static void main(String[] args) throws Exception {
        Config config = Config.from(args);
        if (config.seedOrders()) {
            seedOrders(config);
        }
        Result result = run(config);
        printJson(config, result);
        if (result.failures() > 0) {
            throw new IllegalStateException("Order DB ceiling probe failed rows=" + result.failures());
        }
    }

    private static void seedOrders(Config config) throws SQLException {
        try (Connection connection = DriverManager.getConnection(
                config.jdbcUrl(),
                config.username(),
                config.password());
             PreparedStatement statement = connection.prepareStatement(SEED_MATCHING_STATE_SQL)) {
            connection.setAutoCommit(false);
            int batched = 0;
            for (int index = 1; index <= config.events(); index++) {
                bindSeedOrder(statement, buyerOrderId(index), buyerId(index), config.initialQuantity());
                statement.addBatch();
                bindSeedOrder(statement, sellerOrderId(index), sellerId(index), config.initialQuantity());
                statement.addBatch();
                batched += 2;
                if (batched >= 1000) {
                    statement.executeBatch();
                    connection.commit();
                    batched = 0;
                }
            }
            if (batched > 0) {
                statement.executeBatch();
                connection.commit();
            }
        }
    }

    private static void bindSeedOrder(PreparedStatement statement, UUID orderId, UUID userId, int quantity)
            throws SQLException {
        statement.setObject(1, orderId);
        statement.setObject(2, userId);
        statement.setInt(3, quantity);
    }

    private static Result run(Config config) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(config.workers());
        CountDownLatch done = new CountDownLatch(config.events());
        AtomicInteger next = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger batches = new AtomicInteger();
        List<Long> batchLatenciesNanos = Collections.synchronizedList(new ArrayList<>());

        long started = System.nanoTime();
        for (int worker = 0; worker < config.workers(); worker++) {
            executor.execute(() -> runWorker(config, next, done, failures, batches, batchLatenciesNanos));
        }
        done.await();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        double elapsedSeconds = (System.nanoTime() - started) / 1_000_000_000.0;

        List<Long> sorted = new ArrayList<>(batchLatenciesNanos);
        Collections.sort(sorted);
        return new Result(
                config.events() - failures.get(),
                failures.get(),
                batches.get(),
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
            AtomicInteger batches,
            List<Long> batchLatenciesNanos) {
        try (Connection connection = DriverManager.getConnection(
                config.jdbcUrl(),
                config.username(),
                config.password())) {
            connection.setAutoCommit(config.mode() == Mode.AUTOCOMMIT);
            while (true) {
                int start = next.getAndAdd(config.batchSize());
                if (start >= config.events()) {
                    return;
                }
                int actualBatchSize = Math.min(config.batchSize(), config.events() - start);
                String sql = tradeApplySql(actualBatchSize);
                long batchStarted = System.nanoTime();
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    bindTradeApplyBatch(statement, config.marketId(), start + 1, actualBatchSize);
                    TradeApplyCounts counts = executeTradeApply(statement);
                    if (!counts.completed(actualBatchSize)) {
                        throw new IllegalStateException("unexpected order trade-apply counts=" + counts
                                + ", expectedBatchSize=" + actualBatchSize);
                    }
                    if (config.mode() != Mode.AUTOCOMMIT) {
                        connection.commit();
                    }
                    batches.incrementAndGet();
                    batchLatenciesNanos.add(System.nanoTime() - batchStarted);
                } catch (Exception e) {
                    failures.addAndGet(actualBatchSize);
                    rollbackQuietly(connection, config);
                    if (failures.get() <= 100) {
                        System.err.printf(
                                "order probe batch failed: start=%d, size=%d, error=%s%n",
                                start,
                                actualBatchSize,
                                e.getMessage());
                    }
                } finally {
                    for (int i = 0; i < actualBatchSize; i++) {
                        done.countDown();
                    }
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
            System.err.printf("order probe worker failed: %s%n", e.getMessage());
        }
    }

    private static String tradeApplySql(int batchSize) {
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < batchSize; i++) {
            if (i > 0) {
                values.append(", ");
            }
            values.append("(")
                    .append("?::varchar, ?::uuid, ?::uuid, ?::integer, ")
                    .append("?::integer, ?::timestamp, ")
                    .append("?::uuid, ?::integer, ?::integer, ?::integer, ?::integer, ?::varchar, ")
                    .append("?::uuid, ?::integer, ?::integer, ?::integer, ?::integer, ?::varchar")
                    .append(")");
        }
        return """
                WITH input(trade_id, trade_buyer_order_id, trade_seller_order_id, trade_price,
                           trade_quantity, trade_applied_at,
                           buyer_order_id, buyer_quantity, buyer_previous_remaining_amount,
                           buyer_remaining_amount, buyer_matched_amount, buyer_status,
                           seller_order_id, seller_quantity, seller_previous_remaining_amount,
                           seller_remaining_amount, seller_matched_amount, seller_status) AS (
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
                )
                SELECT
                    (SELECT count FROM existing_trade_applications) AS existing_trade_applications,
                    (SELECT COUNT(*) FROM trade_application) AS inserted_trade_applications,
                    (SELECT COUNT(*) FROM updated_matching_states) AS updated_matching_states
                """;
    }

    private static void bindTradeApplyBatch(
            PreparedStatement statement,
            String marketId,
            int firstSequence,
            int batchSize) throws SQLException {
        int param = 1;
        for (int offset = 0; offset < batchSize; offset++) {
            long sequence = firstSequence + offset;
            String tradeId = marketId + "-" + sequence;
            UUID buyerOrderId = buyerOrderId(sequence);
            UUID sellerOrderId = sellerOrderId(sequence);

            statement.setString(param++, tradeId);
            statement.setObject(param++, buyerOrderId);
            statement.setObject(param++, sellerOrderId);
            statement.setInt(param++, 100);
            statement.setInt(param++, 1);
            statement.setTimestamp(param++, Timestamp.valueOf(LocalDateTime.now()));
            statement.setObject(param++, buyerOrderId);
            statement.setInt(param++, 1);
            statement.setInt(param++, 1);
            statement.setInt(param++, 0);
            statement.setInt(param++, 1);
            statement.setString(param++, "FILLED");
            statement.setObject(param++, sellerOrderId);
            statement.setInt(param++, 1);
            statement.setInt(param++, 1);
            statement.setInt(param++, 0);
            statement.setInt(param++, 1);
            statement.setString(param++, "FILLED");
        }
    }

    private static TradeApplyCounts executeTradeApply(PreparedStatement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery()) {
            if (!rs.next()) {
                return new TradeApplyCounts(0, 0, 0);
            }
            return new TradeApplyCounts(
                    rs.getInt("existing_trade_applications"),
                    rs.getInt("inserted_trade_applications"),
                    rs.getInt("updated_matching_states"));
        }
    }

    private static UUID buyerId(long sequence) {
        return uuid(sequence, 1);
    }

    private static UUID sellerId(long sequence) {
        return uuid(sequence, 2);
    }

    private static UUID buyerOrderId(long sequence) {
        return uuid(sequence, 3);
    }

    private static UUID sellerOrderId(long sequence) {
        return uuid(sequence, 4);
    }

    private static UUID uuid(long sequence, long salt) {
        return new UUID(sequence, salt);
    }

    private static void rollbackQuietly(Connection connection, Config config) {
        if (config.mode() == Mode.AUTOCOMMIT) {
            return;
        }
        try {
            connection.rollback();
        } catch (Exception ignored) {
        }
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
        System.out.printf("  \"mode\": \"orderDbCeilingProbe\",%n");
        System.out.printf("  \"transactionMode\": \"%s\",%n", config.mode().name().toLowerCase());
        System.out.printf("  \"marketId\": \"%s\",%n", config.marketId());
        System.out.printf("  \"events\": %d,%n", config.events());
        System.out.printf("  \"workers\": %d,%n", config.workers());
        System.out.printf("  \"batchSize\": %d,%n", config.batchSize());
        System.out.printf("  \"seedOrders\": %s,%n", config.seedOrders());
        System.out.printf("  \"completed\": %d,%n", result.completed());
        System.out.printf("  \"failures\": %d,%n", result.failures());
        System.out.printf("  \"batches\": %d,%n", result.batches());
        System.out.printf("  \"elapsedSeconds\": %.3f,%n", result.elapsedSeconds());
        System.out.printf("  \"tradeApplyTps\": %.2f,%n", result.completed() / Math.max(result.elapsedSeconds(), 0.001));
        System.out.printf("  \"batchP50Ms\": %.3f,%n", result.batchP50Ms());
        System.out.printf("  \"batchP95Ms\": %.3f,%n", result.batchP95Ms());
        System.out.printf("  \"batchP99Ms\": %.3f%n", result.batchP99Ms());
        System.out.println("}");
    }

    private enum Mode {
        AUTOCOMMIT,
        TRANSACTION_PER_BATCH
    }

    private record TradeApplyCounts(
            int existingTradeApplications,
            int insertedTradeApplications,
            int updatedMatchingStates) {
        boolean completed(int batchSize) {
            return existingTradeApplications == 0
                    && insertedTradeApplications == batchSize
                    && updatedMatchingStates == batchSize * 2;
        }
    }

    private record Result(
            int completed,
            int failures,
            int batches,
            double elapsedSeconds,
            double batchP50Ms,
            double batchP95Ms,
            double batchP99Ms) {
    }

    private record Config(
            String jdbcUrl,
            String username,
            String password,
            String marketId,
            int events,
            int workers,
            int batchSize,
            int initialQuantity,
            boolean seedOrders,
            Mode mode) {

        private static Config from(String[] args) {
            Mode mode = parseMode(stringArg(args, "--mode", "transaction_per_batch"));
            return new Config(
                    stringArg(args, "--jdbc-url", DEFAULT_JDBC_URL),
                    stringArg(args, "--username", DEFAULT_USERNAME),
                    stringArg(args, "--password", DEFAULT_PASSWORD),
                    stringArg(args, "--market-id", "ORDER_DB_CEILING_" + UUID.randomUUID()),
                    intArg(args, "--events", 10_000),
                    intArg(args, "--workers", 16),
                    intArg(args, "--batch-size", 100),
                    intArg(args, "--initial-quantity", 1),
                    booleanArg(args, "--seed-orders", true),
                    mode);
        }

        private static Mode parseMode(String value) {
            String normalized = value.toUpperCase();
            if ("TRANSACTION_PER_ROW".equals(normalized)) {
                return Mode.TRANSACTION_PER_BATCH;
            }
            return Mode.valueOf(normalized);
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
