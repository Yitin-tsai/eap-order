package com.eap.eap_order.loadtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.eap.common.constants.RabbitMQConstants.DEAD_LETTER_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.MATCH_ENGINE_ORDER_CONFIRMED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_CONFIRMED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_FAILED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_ORDER_SUBMITTED_QUEUE;

public class OrderHttpLoadGenerator {

    private static final String DEFAULT_ORDER_URL = "http://localhost:8080/eap-order";
    private static final String DEFAULT_WALLET_URL = "http://localhost:8081/eap-wallet";
    private static final String DEFAULT_ORDER_JDBC_URL = "jdbc:postgresql://localhost:15432/eap_order_db";
    private static final String DEFAULT_WALLET_JDBC_URL = "jdbc:postgresql://localhost:15433/eap_wallet_db";
    private static final String DEFAULT_RABBIT_MANAGEMENT_URL = "http://localhost:15672";
    private static final List<String> ORDER_ADMISSION_QUEUES = List.of(
            WALLET_ORDER_SUBMITTED_QUEUE,
            ORDER_ORDER_CONFIRMED_QUEUE,
            ORDER_ORDER_FAILED_QUEUE,
            MATCH_ENGINE_ORDER_CONFIRMED_QUEUE,
            DEAD_LETTER_QUEUE
    );

    public static void main(String[] args) throws Exception {
        Config config = Config.from(args);
        ObjectMapper objectMapper = new ObjectMapper();
        ExecutorService httpExecutor = Executors.newFixedThreadPool(config.workers());
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        if (config.resetData()) {
            resetOrderAdmissionData(config, httpClient, objectMapper);
        } else if (config.orderAdmissionGate()) {
            redisDel(config, orderbookKey(config.marketId(), "buy"), orderbookKey(config.marketId(), "sell"));
        }

        System.out.printf("registering %d users through %s%n", config.users(), config.walletUrl());
        List<UUID> users = registerUsers(config, httpClient, objectMapper);
        System.out.printf("registered users: %d%n", users.size());

        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger tooManyRequests = new AtomicInteger();
        AtomicInteger unavailable = new AtomicInteger();
        AtomicInteger otherFailures = new AtomicInteger();
        AtomicLong nextSendAtNanos = new AtomicLong(System.nanoTime());
        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / Math.max(config.tps(), 1);
        List<Long> latenciesNanos = Collections.synchronizedList(new ArrayList<>(config.events()));
        List<UUID> orderIds = new ArrayList<>(config.events());
        for (int i = 0; i < config.events(); i++) {
            orderIds.add(deterministicOrderId(config.runId(), i));
        }
        CountDownLatch done = new CountDownLatch(config.events());
        Semaphore inFlight = new Semaphore(config.workers() * 2);
        long startedAt = System.nanoTime();
        ScheduledExecutorService progressReporter = Executors.newSingleThreadScheduledExecutor();
        progressReporter.scheduleAtFixedRate(() -> {
            long completed = config.events() - done.getCount();
            double elapsed = (System.nanoTime() - startedAt) / 1_000_000_000.0;
            System.out.printf(
                    "progress completed=%d/%d accepted=%d 429=%d 503=%d failures=%d averageTps=%.2f%n",
                    completed,
                    config.events(),
                    accepted.get(),
                    tooManyRequests.get(),
                    unavailable.get(),
                    otherFailures.get(),
                    accepted.get() / Math.max(elapsed, 0.001));
        }, 30, 30, TimeUnit.SECONDS);

        System.out.printf(
                "sending %d HTTP orders, targetTps=%d, workers=%d, side=%s, mode=%s%n",
                config.events(), config.tps(), config.workers(), config.side(), config.mode());

        for (int i = 0; i < config.events(); i++) {
            int index = i;
            throttle(nextSendAtNanos, intervalNanos);
            inFlight.acquire();
            httpExecutor.execute(() -> {
                try {
                    UUID userId = users.get(index % users.size());
                    boolean buy = "BUY".equals(config.side());
                    UUID orderId = orderIds.get(index);
                    String body = buy
                            ? objectMapper.writeValueAsString(new BuyRequest(orderId, 10, 1, userId))
                            : objectMapper.writeValueAsString(new SellRequest(orderId, 10, 1, userId));
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(config.orderUrl() + (buy ? "/bid/buy" : "/bid/sell")))
                            .timeout(Duration.ofSeconds(10))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build();

                    long requestStarted = System.nanoTime();
                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    latenciesNanos.add(System.nanoTime() - requestStarted);
                    switch (response.statusCode()) {
                        case 200, 202 -> accepted.incrementAndGet();
                        case 429 -> tooManyRequests.incrementAndGet();
                        case 503 -> unavailable.incrementAndGet();
                        default -> {
                            otherFailures.incrementAndGet();
                            if (otherFailures.get() <= 10) {
                                System.err.printf("unexpected response: status=%d, body=%s%n",
                                        response.statusCode(), response.body());
                            }
                        }
                    }
                } catch (Exception e) {
                    otherFailures.incrementAndGet();
                    if (otherFailures.get() <= 10) {
                        System.err.printf("request failed: %s%n", e.getMessage());
                    }
                } finally {
                    inFlight.release();
                    done.countDown();
                }
            });
        }

        done.await();
        progressReporter.shutdownNow();
        httpExecutor.shutdown();
        httpExecutor.awaitTermination(30, TimeUnit.SECONDS);

        double elapsedSeconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
        List<Long> sortedLatencies = new ArrayList<>(latenciesNanos);
        Collections.sort(sortedLatencies);
        AdmissionSnapshot admission = config.orderAdmissionGate()
                ? waitForOrderAdmission(config, httpClient, objectMapper, orderIds, startedAt)
                : AdmissionSnapshot.disabled(elapsedSeconds);
        List<String> invalidReasons = invalidReasons(config, accepted.get(), tooManyRequests.get(),
                unavailable.get(), otherFailures.get(), admission);

        System.out.println("{");
        System.out.printf("  \"benchmarkSchemaVersion\": 2,%n");
        System.out.printf("  \"mode\": \"%s\",%n", json(config.mode()));
        System.out.printf("  \"runId\": \"%s\",%n", json(config.runId()));
        System.out.printf("  \"events\": %d,%n", config.events());
        System.out.printf("  \"side\": \"%s\",%n", json(config.side()));
        System.out.printf("  \"users\": %d,%n", config.users());
        System.out.printf("  \"targetTps\": %d,%n", config.tps());
        System.out.printf("  \"resetData\": %s,%n", config.resetData());
        System.out.printf("  \"httpAccepted\": %d,%n", accepted.get());
        System.out.printf("  \"http429\": %d,%n", tooManyRequests.get());
        System.out.printf("  \"http503\": %d,%n", unavailable.get());
        System.out.printf("  \"otherFailures\": %d,%n", otherFailures.get());
        System.out.printf("  \"httpSendElapsedSeconds\": %.2f,%n", elapsedSeconds);
        System.out.printf("  \"httpAcceptedTps\": %.2f,%n", accepted.get() / Math.max(elapsedSeconds, 0.001));
        System.out.printf("  \"httpAcceptedP50Ms\": %.2f,%n", percentileMillis(sortedLatencies, 0.50));
        System.out.printf("  \"httpAcceptedP95Ms\": %.2f,%n", percentileMillis(sortedLatencies, 0.95));
        System.out.printf("  \"httpAcceptedP99Ms\": %.2f,%n", percentileMillis(sortedLatencies, 0.99));
        System.out.printf("  \"orderSubmissionRequestedRows\": %d,%n", admission.submissionRequestedRows());
        System.out.printf("  \"orderSubmissionRequestedReachedSeconds\": %.2f,%n", admission.submissionRequestedReachedSeconds());
        System.out.printf("  \"orderSubmittedOutboxSentRows\": %d,%n", admission.orderSubmittedOutboxSentRows());
        System.out.printf("  \"orderSubmittedOutboxSentReachedSeconds\": %.2f,%n", admission.orderSubmittedOutboxSentReachedSeconds());
        System.out.printf("  \"walletOrderSubmissionClaimRows\": %d,%n", admission.walletOrderSubmissionClaimRows());
        System.out.printf("  \"walletOrderSubmissionClaimReachedSeconds\": %.2f,%n", admission.walletOrderSubmissionClaimReachedSeconds());
        System.out.printf("  \"orderAssetReservationConfirmedRows\": %d,%n", admission.assetReservationConfirmedRows());
        System.out.printf("  \"orderAssetReservationConfirmedReachedSeconds\": %.2f,%n", admission.assetReservationConfirmedReachedSeconds());
        System.out.printf("  \"matchEngineOrderbookAdmissionCount\": %d,%n", admission.orderbookAdmissionCount());
        System.out.printf("  \"matchEngineOrderbookAdmissionReachedSeconds\": %.2f,%n", admission.orderbookAdmissionReachedSeconds());
        System.out.printf("  \"matchEngineOrderbookAdmissionTps\": %.2f,%n",
                admission.orderbookAdmissionCount() / Math.max(admission.orderbookAdmissionReachedSeconds(), 0.001));
        System.out.printf("  \"finalQueueDrainReachedSeconds\": %.2f,%n", admission.queueDrainReachedSeconds());
        System.out.printf("  \"lastNonZeroQueues\": \"%s\",%n", json(admission.lastNonZeroQueues()));
        System.out.printf("  \"lastNonZeroQueuesObservedSeconds\": %.2f,%n", admission.lastNonZeroQueuesObservedSeconds());
        System.out.printf("  \"orderAdmissionGateElapsedSeconds\": %.2f,%n", admission.elapsedSeconds());
        System.out.printf("  \"businessOrderAdmissionTps\": %.2f,%n",
                admission.orderbookAdmissionCount() / Math.max(admission.elapsedSeconds(), 0.001));
        System.out.printf("  \"finalQueueBacklog\": %d,%n", admission.finalQueueBacklog());
        System.out.printf("  \"queueMetricsReadFailures\": %d,%n", admission.queueMetricsReadFailures());
        System.out.printf("  \"validForCapacityComparison\": %s,%n", invalidReasons.isEmpty());
        System.out.printf("  \"capacityInvalidReasons\": %s%n", jsonArray(invalidReasons));
        System.out.println("}");
    }

    private static List<UUID> registerUsers(Config config, HttpClient httpClient, ObjectMapper objectMapper)
            throws Exception {
        List<UUID> users = new ArrayList<>(config.users());
        for (int i = 0; i < config.users(); i++) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(config.walletUrl() + "/v1/wallet/register"))
                    .timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("wallet registration failed: " + response.body());
            }
            JsonNode body = objectMapper.readTree(response.body());
            users.add(UUID.fromString(body.path("userId").asText()));
        }
        return users;
    }

    private static void resetOrderAdmissionData(Config config, HttpClient httpClient, ObjectMapper objectMapper)
            throws Exception {
        System.out.println("resetting order-admission benchmark data");
        purgeQueues(config, httpClient, objectMapper);
        truncateOrderAdmissionOrderData(config);
        truncateOrderAdmissionWalletData(config);
        purgeQueues(config, httpClient, objectMapper);
        redisDel(config,
                orderbookKey(config.marketId(), "buy"),
                orderbookKey(config.marketId(), "sell"));
    }

    private static void truncateOrderAdmissionOrderData(Config config) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                config.orderJdbcUrl(), config.orderJdbcUser(), config.orderJdbcPassword());
             PreparedStatement statement = connection.prepareStatement("""
                     TRUNCATE TABLE
                         order_service.match_history,
                         order_service.order_trade_execution_inbox,
                         order_service.order_trade_applications,
                         order_service.order_event_outbox,
                         order_service.order_matching_state,
                         order_service.orders_current,
                         order_service.projection_checkpoints,
                         order_service.order_event_store,
                         order_service.order_stream_heads
                     RESTART IDENTITY CASCADE
                     """)) {
            statement.execute();
        }
    }

    private static void truncateOrderAdmissionWalletData(Config config) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                config.walletJdbcUrl(), config.walletJdbcUser(), config.walletJdbcPassword());
             PreparedStatement statement = connection.prepareStatement("""
                     TRUNCATE TABLE
                         wallet_service.trade_settlements,
                         wallet_service.outbox,
                         wallet_service.order_submission_idempotency,
                         wallet_service.settlement_idempotency,
                         wallet_service.wallets
                     RESTART IDENTITY CASCADE
                     """)) {
            statement.execute();
        }
    }

    private static void purgeQueues(Config config, HttpClient httpClient, ObjectMapper objectMapper) {
        for (String queue : ORDER_ADMISSION_QUEUES) {
            purgeQueue(config, httpClient, queue);
        }
    }

    private static void purgeQueue(Config config, HttpClient httpClient, String queue) {
        try {
            String encodedQueue = URLEncoder.encode(queue, StandardCharsets.UTF_8).replace("+", "%20");
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(config.rabbitManagementUrl()
                            + "/api/queues/%2F/" + encodedQueue + "/contents"))
                    .timeout(Duration.ofSeconds(5))
                    .header("Authorization", basicAuth(config.rabbitManagementUser(), config.rabbitManagementPassword()))
                    .DELETE()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 404) {
                throw new IllegalStateException("RabbitMQ queue not found during purge: " + queue);
            }
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException(
                        "RabbitMQ queue purge failed: queue=" + queue + ", status=" + response.statusCode());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to purge RabbitMQ queue " + queue, e);
        }
    }

    private static AdmissionSnapshot waitForOrderAdmission(
            Config config,
            HttpClient httpClient,
            ObjectMapper objectMapper,
            List<UUID> orderIds,
            long startedAtNanos) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.waitTimeoutSeconds());
        long submissionRequestedRows = 0;
        long orderSubmittedOutboxSentRows = 0;
        long walletOrderSubmissionClaimRows = 0;
        long assetReservationConfirmedRows = 0;
        long orderbookAdmissionCount = 0;
        long finalQueueBacklog = Long.MAX_VALUE;
        long queueMetricsReadFailures = 0;
        String lastNonZeroQueues = "";
        double lastNonZeroQueuesObservedSeconds = 0;
        double submissionRequestedReachedSeconds = 0;
        double orderSubmittedOutboxSentReachedSeconds = 0;
        double walletOrderSubmissionClaimReachedSeconds = 0;
        double assetReservationConfirmedReachedSeconds = 0;
        double orderbookAdmissionReachedSeconds = 0;
        double queueDrainReachedSeconds = 0;
        while (System.nanoTime() < deadlineNanos) {
            submissionRequestedRows = countOrderEvents(config, orderIds, "OrderSubmissionRequestedV1");
            if (submissionRequestedRows == config.events() && submissionRequestedReachedSeconds == 0) {
                submissionRequestedReachedSeconds = elapsedSince(startedAtNanos);
            }
            orderSubmittedOutboxSentRows = countOrderSubmittedOutbox(config, orderIds, "SENT");
            if (orderSubmittedOutboxSentRows == config.events() && orderSubmittedOutboxSentReachedSeconds == 0) {
                orderSubmittedOutboxSentReachedSeconds = elapsedSince(startedAtNanos);
            }
            walletOrderSubmissionClaimRows = countWalletOrderSubmissionClaims(config, orderIds);
            if (walletOrderSubmissionClaimRows == config.events() && walletOrderSubmissionClaimReachedSeconds == 0) {
                walletOrderSubmissionClaimReachedSeconds = elapsedSince(startedAtNanos);
            }
            assetReservationConfirmedRows = countOrderEvents(config, orderIds, "OrderAssetReservationConfirmedV1");
            if (assetReservationConfirmedRows == config.events() && assetReservationConfirmedReachedSeconds == 0) {
                assetReservationConfirmedReachedSeconds = elapsedSince(startedAtNanos);
            }
            orderbookAdmissionCount = redisZcard(config, orderbookKey(config.marketId(), config.side().toLowerCase(Locale.ROOT)));
            if (orderbookAdmissionCount == config.events() && orderbookAdmissionReachedSeconds == 0) {
                orderbookAdmissionReachedSeconds = elapsedSince(startedAtNanos);
            }
            QueueSnapshot queueSnapshot = readQueues(config, httpClient, objectMapper);
            finalQueueBacklog = queueSnapshot.backlog();
            queueMetricsReadFailures += queueSnapshot.readFailures();
            if (finalQueueBacklog > 0) {
                lastNonZeroQueues = queueSnapshot.nonZeroQueues();
                lastNonZeroQueuesObservedSeconds = elapsedSince(startedAtNanos);
            }
            if (finalQueueBacklog == 0 && queueMetricsReadFailures == 0 && queueDrainReachedSeconds == 0) {
                queueDrainReachedSeconds = elapsedSince(startedAtNanos);
            }
            if (submissionRequestedRows == config.events()
                    && orderSubmittedOutboxSentRows == config.events()
                    && walletOrderSubmissionClaimRows == config.events()
                    && assetReservationConfirmedRows == config.events()
                    && orderbookAdmissionCount == config.events()
                    && finalQueueBacklog == 0
                    && queueMetricsReadFailures == 0) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(500);
        }
        double elapsedSeconds = (System.nanoTime() - startedAtNanos) / 1_000_000_000.0;
        return new AdmissionSnapshot(
                submissionRequestedRows,
                orderSubmittedOutboxSentRows,
                walletOrderSubmissionClaimRows,
                assetReservationConfirmedRows,
                orderbookAdmissionCount,
                finalQueueBacklog,
                queueMetricsReadFailures,
                submissionRequestedReachedSeconds,
                orderSubmittedOutboxSentReachedSeconds,
                walletOrderSubmissionClaimReachedSeconds,
                assetReservationConfirmedReachedSeconds,
                orderbookAdmissionReachedSeconds,
                queueDrainReachedSeconds,
                lastNonZeroQueues,
                lastNonZeroQueuesObservedSeconds,
                elapsedSeconds);
    }

    private static long countOrderEvents(Config config, List<UUID> orderIds, String eventType) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                config.orderJdbcUrl(), config.orderJdbcUser(), config.orderJdbcPassword());
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT count(*)
                     FROM order_service.order_event_store
                     WHERE event_type = ?
                       AND aggregate_id = ANY(?)
                     """)) {
            statement.setString(1, eventType);
            UUID[] uuidArray = orderIds.toArray(UUID[]::new);
            Array sqlArray = connection.createArrayOf("uuid", uuidArray);
            statement.setArray(2, sqlArray);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getLong(1) : 0;
            } finally {
                sqlArray.free();
            }
        }
    }

    private static long countOrderSubmittedOutbox(Config config, List<UUID> orderIds, String status) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                config.orderJdbcUrl(), config.orderJdbcUser(), config.orderJdbcPassword());
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT count(*)
                     FROM order_service.order_event_outbox outbox
                     JOIN order_service.order_event_store event_store
                       ON event_store.event_id = outbox.event_id
                     WHERE event_store.event_type = 'OrderSubmissionRequestedV1'
                       AND event_store.aggregate_id = ANY(?)
                       AND outbox.status = ?
                     """)) {
            UUID[] uuidArray = orderIds.toArray(UUID[]::new);
            Array sqlArray = connection.createArrayOf("uuid", uuidArray);
            statement.setArray(1, sqlArray);
            statement.setString(2, status);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getLong(1) : 0;
            } finally {
                sqlArray.free();
            }
        }
    }

    private static long countWalletOrderSubmissionClaims(Config config, List<UUID> orderIds) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                config.walletJdbcUrl(), config.walletJdbcUser(), config.walletJdbcPassword());
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT count(*)
                     FROM wallet_service.order_submission_idempotency
                     WHERE order_id = ANY(?)
                     """)) {
            UUID[] uuidArray = orderIds.toArray(UUID[]::new);
            Array sqlArray = connection.createArrayOf("uuid", uuidArray);
            statement.setArray(1, sqlArray);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getLong(1) : 0;
            } finally {
                sqlArray.free();
            }
        }
    }

    private static double elapsedSince(long startedAtNanos) {
        return (System.nanoTime() - startedAtNanos) / 1_000_000_000.0;
    }

    private static QueueSnapshot readQueues(Config config, HttpClient httpClient, ObjectMapper objectMapper) {
        long backlog = 0;
        long failures = 0;
        StringJoiner nonZeroQueues = new StringJoiner(",");
        for (String queue : ORDER_ADMISSION_QUEUES) {
            try {
                QueueDepth depth = readQueue(config, httpClient, objectMapper, queue);
                backlog += depth.ready() + depth.unacked();
                if (depth.ready() + depth.unacked() > 0) {
                    nonZeroQueues.add(queue + "(ready=" + depth.ready() + ",unacked=" + depth.unacked() + ")");
                }
            } catch (Exception e) {
                failures++;
                if (failures <= 3) {
                    System.err.printf("queue metrics read failed: queue=%s, error=%s%n", queue, e.getMessage());
                }
            }
        }
        return new QueueSnapshot(backlog, failures, nonZeroQueues.toString());
    }

    private static QueueDepth readQueue(Config config, HttpClient httpClient, ObjectMapper objectMapper, String queue)
            throws Exception {
        String encodedQueue = URLEncoder.encode(queue, StandardCharsets.UTF_8).replace("+", "%20");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(config.rabbitManagementUrl()
                        + "/api/queues/%2F/" + encodedQueue
                        + "?columns=name,messages_ready,messages_unacknowledged"))
                .timeout(Duration.ofSeconds(3))
                .header("Authorization", basicAuth(config.rabbitManagementUser(), config.rabbitManagementPassword()))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 404) {
            throw new IllegalStateException("RabbitMQ queue not found: " + queue);
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("RabbitMQ management status=" + response.statusCode());
        }
        JsonNode body = objectMapper.readTree(response.body());
        return new QueueDepth(
                body.path("messages_ready").asLong(0),
                body.path("messages_unacknowledged").asLong(0));
    }

    private static String basicAuth(String user, String password) {
        return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    private static String orderbookKey(String marketId, String side) {
        return "orderbook:" + marketId + ":" + side;
    }

    private static UUID deterministicOrderId(String runId, int index) {
        return UUID.nameUUIDFromBytes((runId + ":order:" + index).getBytes(StandardCharsets.UTF_8));
    }

    private static void redisDel(Config config, String... keys) throws IOException {
        redisCommand(config, command("DEL", keys));
    }

    private static long redisZcard(Config config, String key) throws IOException {
        return Long.parseLong(redisCommand(config, command("ZCARD", key)).trim());
    }

    private static String redisCommand(Config config, byte[] command) throws IOException {
        try (Socket socket = new Socket(config.redisHost(), config.redisPort())) {
            socket.setSoTimeout(3_000);
            OutputStream out = socket.getOutputStream();
            out.write(command);
            out.flush();
            return readRedisResponse(socket.getInputStream());
        }
    }

    private static byte[] command(String name, String... args) {
        StringBuilder builder = new StringBuilder();
        builder.append("*").append(args.length + 1).append("\r\n");
        appendBulk(builder, name);
        for (String arg : args) {
            appendBulk(builder, arg);
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void appendBulk(StringBuilder builder, String value) {
        builder.append("$").append(value.getBytes(StandardCharsets.UTF_8).length).append("\r\n")
                .append(value).append("\r\n");
    }

    private static String readRedisResponse(InputStream input) throws IOException {
        int prefix = input.read();
        if (prefix == -1) {
            throw new IOException("empty Redis response");
        }
        String line = readLine(input);
        if (prefix == '-') {
            throw new IOException("Redis error: " + line);
        }
        return line;
    }

    private static String readLine(InputStream input) throws IOException {
        StringBuilder builder = new StringBuilder();
        int previous = -1;
        int current;
        while ((current = input.read()) != -1) {
            if (previous == '\r' && current == '\n') {
                builder.setLength(builder.length() - 1);
                return builder.toString();
            }
            builder.append((char) current);
            previous = current;
        }
        throw new IOException("unterminated Redis response");
    }

    private static List<String> invalidReasons(
            Config config,
            int accepted,
            int tooManyRequests,
            int unavailable,
            int otherFailures,
            AdmissionSnapshot admission) {
        List<String> reasons = new ArrayList<>();
        if (accepted != config.events()) {
            reasons.add("http_accepted_not_equal_events");
        }
        if (tooManyRequests != 0) {
            reasons.add("http_429");
        }
        if (unavailable != 0) {
            reasons.add("http_503");
        }
        if (otherFailures != 0) {
            reasons.add("http_other_failures");
        }
        if (!config.orderAdmissionGate()) {
            return reasons;
        }
        if (admission.submissionRequestedRows() != config.events()) {
            reasons.add("order_submission_requested_not_equal_events");
        }
        if (admission.orderSubmittedOutboxSentRows() != config.events()) {
            reasons.add("order_submitted_outbox_sent_not_equal_events");
        }
        if (admission.walletOrderSubmissionClaimRows() != config.events()) {
            reasons.add("wallet_order_submission_claim_not_equal_events");
        }
        if (admission.assetReservationConfirmedRows() != config.events()) {
            reasons.add("order_asset_reservation_confirmed_not_equal_events");
        }
        if (admission.orderbookAdmissionCount() != config.events()) {
            reasons.add("match_engine_orderbook_admission_not_equal_events");
        }
        if (admission.finalQueueBacklog() != 0) {
            reasons.add("final_queue_backlog_not_zero");
        }
        if (admission.queueMetricsReadFailures() != 0) {
            reasons.add("queue_metrics_read_failures");
        }
        return reasons;
    }

    private static void throttle(AtomicLong nextSendAtNanos, long intervalNanos) {
        long now = System.nanoTime();
        long scheduledAt = nextSendAtNanos.getAndUpdate(previous ->
                Math.max(previous, now) + intervalNanos);
        scheduledAt = Math.max(scheduledAt, now);
        long waitNanos = scheduledAt - System.nanoTime();
        if (waitNanos > 0) {
            try {
                TimeUnit.NANOSECONDS.sleep(waitNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while throttling", e);
            }
        }
    }

    private static double percentileMillis(List<Long> sorted, double percentile) {
        if (sorted.isEmpty()) {
            return 0;
        }
        int index = (int) Math.ceil(percentile * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1))) / 1_000_000.0;
    }

    private static String json(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String jsonArray(List<String> values) {
        return values.stream()
                .map(value -> "\"" + json(value) + "\"")
                .reduce("[", (left, right) -> left.equals("[") ? left + right : left + ", " + right) + "]";
    }

    private record BuyRequest(UUID orderId, int bidPrice, int amount, UUID bidder) {
    }

    private record SellRequest(UUID orderId, int sellPrice, int amount, UUID seller) {
    }

    private record QueueDepth(long ready, long unacked) {
    }

    private record QueueSnapshot(long backlog, long readFailures, String nonZeroQueues) {
    }

    private record AdmissionSnapshot(
            long submissionRequestedRows,
            long orderSubmittedOutboxSentRows,
            long walletOrderSubmissionClaimRows,
            long assetReservationConfirmedRows,
            long orderbookAdmissionCount,
            long finalQueueBacklog,
            long queueMetricsReadFailures,
            double submissionRequestedReachedSeconds,
            double orderSubmittedOutboxSentReachedSeconds,
            double walletOrderSubmissionClaimReachedSeconds,
            double assetReservationConfirmedReachedSeconds,
            double orderbookAdmissionReachedSeconds,
            double queueDrainReachedSeconds,
            String lastNonZeroQueues,
            double lastNonZeroQueuesObservedSeconds,
        double elapsedSeconds) {
        static AdmissionSnapshot disabled(double elapsedSeconds) {
            return new AdmissionSnapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", 0, elapsedSeconds);
        }
    }

    private record Config(
            String mode,
            String runId,
            String side,
            int users,
            int events,
            int tps,
            int workers,
            int waitTimeoutSeconds,
            boolean orderAdmissionGate,
            boolean resetData,
            String marketId,
            String orderUrl,
            String walletUrl,
            String orderJdbcUrl,
            String orderJdbcUser,
            String orderJdbcPassword,
            String walletJdbcUrl,
            String walletJdbcUser,
            String walletJdbcPassword,
            String redisHost,
            int redisPort,
            String rabbitManagementUrl,
            String rabbitManagementUser,
            String rabbitManagementPassword) {
        private static Config from(String[] args) {
            int tps = intArg(args, "--tps", 1000);
            int durationSeconds = intArg(args, "--duration-seconds", 0);
            int events = durationSeconds > 0
                    ? Math.multiplyExact(tps, durationSeconds)
                    : intArg(args, "--events", 10_000);
            String mode = stringArg(args, "--mode", "orderAdmissionChain");
            String side = stringArg(args, "--side", "SELL").toUpperCase(Locale.ROOT);
            if (!"BUY".equals(side) && !"SELL".equals(side)) {
                throw new IllegalArgumentException("--side must be BUY or SELL");
            }
            return new Config(
                    mode,
                    stringArg(args, "--run-id", "ORDER_ADMISSION_" + Instant.now().toEpochMilli()),
                    side,
                    intArg(args, "--users", 500),
                    events,
                    tps,
                    intArg(args, "--workers", 128),
                    intArg(args, "--wait-timeout-seconds", 120),
                    booleanArg(args, "--order-admission-gate", true),
                    booleanArg(args, "--reset-data", true),
                    stringArg(args, "--market-id", "ENERGY-SPOT"),
                    stringArg(args, "--order-url", DEFAULT_ORDER_URL),
                    stringArg(args, "--wallet-url", DEFAULT_WALLET_URL),
                    stringArg(args, "--order-jdbc-url", DEFAULT_ORDER_JDBC_URL),
                    stringArg(args, "--order-jdbc-user", "admin"),
                    stringArg(args, "--order-jdbc-password", "admin123"),
                    stringArg(args, "--wallet-jdbc-url", DEFAULT_WALLET_JDBC_URL),
                    stringArg(args, "--wallet-jdbc-user", "admin"),
                    stringArg(args, "--wallet-jdbc-password", "admin123"),
                    stringArg(args, "--redis-host", "localhost"),
                    intArg(args, "--redis-port", 6379),
                    stringArg(args, "--rabbit-management-url", DEFAULT_RABBIT_MANAGEMENT_URL),
                    stringArg(args, "--rabbit-management-user", "admin"),
                    stringArg(args, "--rabbit-management-password", "admin123")
            );
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
