package com.eap.eap_order.loadtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.BufferedWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.eap.common.constants.RabbitMQConstants.DEAD_LETTER_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.MATCH_ENGINE_ORDER_CONFIRMED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.MATCH_ENGINE_ORDER_CANCELLATION_REQUESTED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_CANCELLATION_RESULT_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_CONFIRMED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_FAILED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_TRADE_EXECUTED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_ORDER_SUBMITTED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_ORDER_CANCELLATION_RESULT_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_TRADE_EXECUTED_QUEUE;
import static com.eap.eap_order.loadtest.RabbitManagementClient.QueueDepth;
import static com.eap.eap_order.loadtest.RabbitManagementClient.QueueSnapshot;

public class HttpMatchedTradeCompletionLoadGenerator {

    private static final int PRICE = 100;
    private static final int AMOUNT = 1;
    private static final int BENCHMARK_SCHEMA_VERSION = 2;
    private static final int MAX_STEADY_SCHEDULING_OVERRUN_SECONDS = 30;
    private static final String HTTP_MARKET_ID = "ENERGY-SPOT";
    private static final List<String> CHAIN_QUEUES = List.of(
            WALLET_ORDER_SUBMITTED_QUEUE,
            ORDER_ORDER_CONFIRMED_QUEUE,
            ORDER_ORDER_FAILED_QUEUE,
            MATCH_ENGINE_ORDER_CONFIRMED_QUEUE,
            MATCH_ENGINE_ORDER_CANCELLATION_REQUESTED_QUEUE,
            ORDER_TRADE_EXECUTED_QUEUE,
            WALLET_TRADE_EXECUTED_QUEUE,
            ORDER_ORDER_CANCELLATION_RESULT_QUEUE,
            WALLET_ORDER_CANCELLATION_RESULT_QUEUE,
            DEAD_LETTER_QUEUE
    );

    public static void main(String[] args) throws Exception {
        Config config = Config.from(args);
        if (!HTTP_MARKET_ID.equals(config.marketId())) {
            throw new IllegalArgumentException(
                    "Order HTTP currently assigns market " + HTTP_MARKET_ID + "; --market-id must match");
        }

        ObjectMapper objectMapper = new ObjectMapper();
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        if (config.resetData()) {
            reset(config, httpClient, objectMapper);
        }

        System.out.printf("registering %d buyers and %d sellers through Wallet HTTP%n",
                config.usersPerSide(), config.usersPerSide());
        List<UUID> buyers = registerUsers(config, httpClient, objectMapper, config.usersPerSide());
        List<UUID> sellers = registerUsers(config, httpClient, objectMapper, config.usersPerSide());
        List<UUID> buyOrderIds = orderIds(config.runId(), "BUY", config.events());
        List<UUID> sellOrderIds = orderIds(config.runId(), "SELL", config.events());
        List<UUID> allOrderIds = new ArrayList<>(config.events() * 2);
        allOrderIds.addAll(sellOrderIds);
        allOrderIds.addAll(buyOrderIds);

        try (DatabaseHandles databases = DatabaseHandles.open(config)) {
            RoleBalances initialBuyerBalances = readRoleBalances(databases.wallet(), buyers);
            RoleBalances initialSellerBalances = readRoleBalances(databases.wallet(), sellers);

            long lifecycleStartedAt = System.nanoTime();
            HttpPhaseResult sellHttp = sendOrders(
                    config, httpClient, objectMapper, "SELL", sellOrderIds, sellers);
            SellAdmissionResult sellAdmission = waitForSellAdmission(
                    config, databases, httpClient, objectMapper, sellOrderIds, lifecycleStartedAt);

            long buyStartedAt = System.nanoTime();
            double preBuyLifecycleSeconds =
                    (buyStartedAt - lifecycleStartedAt) / 1_000_000_000.0;
            HttpPhaseResult buyHttp = sendOrders(
                    config, httpClient, objectMapper, "BUY", buyOrderIds, buyers);
            CompletionResult completion = waitForCompletion(
                    config,
                    databases,
                    httpClient,
                    objectMapper,
                    buyOrderIds,
                    sellOrderIds,
                    allOrderIds,
                    buyers,
                    sellers,
                    initialBuyerBalances,
                    initialSellerBalances,
                    buyStartedAt);
            double fullLifecycleSeconds = preBuyLifecycleSeconds + completion.elapsedSeconds();
            double benchmarkWallClockSeconds = elapsedSince(lifecycleStartedAt);

            List<String> invalidReasons = invalidReasons(
                    config,
                    sellHttp,
                    sellAdmission,
                    buyHttp,
                    completion,
                    initialBuyerBalances,
                    initialSellerBalances);
            printResult(
                    config,
                    sellHttp,
                    sellAdmission,
                    buyHttp,
                    completion,
                    initialBuyerBalances,
                    initialSellerBalances,
                    fullLifecycleSeconds,
                    benchmarkWallClockSeconds,
                    invalidReasons);

            if (!invalidReasons.isEmpty()) {
                throw new IllegalStateException("HTTP matched-trade completion benchmark invalid: " + invalidReasons);
            }
        }
    }

    static void runSteadyState(String[] args) throws Exception {
        new SteadyStateRunner(SteadyStateConfig.from(args)).run();
    }

    static void prepareExternalSteadyState(String[] args) throws Exception {
        new ExternalSteadyStateRunner(args).prepare();
    }

    static void monitorExternalSteadyState(String[] args) throws Exception {
        new ExternalSteadyStateRunner(args).monitor();
    }

    static void verifyExternalSteadyState(String[] args) throws Exception {
        new ExternalSteadyStateRunner(args).verify();
    }

    static void resetData(String[] args) throws Exception {
        Config config = Config.from(args);
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        reset(config, httpClient, new ObjectMapper());
    }

    static void runStaircase(String[] args) throws Exception {
        new StaircaseRunner(StaircaseConfig.from(args)).run();
    }

    private static HttpPhaseResult sendOrders(
            Config config,
            HttpClient httpClient,
            ObjectMapper objectMapper,
            String side,
            List<UUID> orderIds,
            List<UUID> users) throws Exception {
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger tooManyRequests = new AtomicInteger();
        AtomicInteger unavailable = new AtomicInteger();
        AtomicInteger otherFailures = new AtomicInteger();
        AtomicLong nextSendAtNanos = new AtomicLong(System.nanoTime());
        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / Math.max(config.targetTps(), 1);
        List<Long> latenciesNanos = Collections.synchronizedList(new ArrayList<>(config.events()));
        CountDownLatch done = new CountDownLatch(config.events());
        Semaphore inFlight = new Semaphore(config.maxInFlight());
        ExecutorService executor = Executors.newFixedThreadPool(config.workers());
        long startedAt = System.nanoTime();

        System.out.printf(
                "sending %d %s orders through HTTP, targetTps=%d, workers=%d, maxInFlight=%d%n",
                config.events(), side, config.targetTps(), config.workers(), config.maxInFlight());
        for (int i = 0; i < config.events(); i++) {
            int index = i;
            throttle(nextSendAtNanos, intervalNanos);
            inFlight.acquire();
            executor.execute(() -> {
                try {
                    UUID userId = users.get(index % users.size());
                    UUID orderId = orderIds.get(index);
                    boolean buy = "BUY".equals(side);
                    String body = buy
                            ? objectMapper.writeValueAsString(new BuyRequest(orderId, PRICE, AMOUNT, userId))
                            : objectMapper.writeValueAsString(new SellRequest(orderId, PRICE, AMOUNT, userId));
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(config.orderUrl() + (buy ? "/bid/buy" : "/bid/sell")))
                            .timeout(Duration.ofSeconds(10))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build();
                    long requestStartedAt = System.nanoTime();
                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    latenciesNanos.add(System.nanoTime() - requestStartedAt);
                    switch (response.statusCode()) {
                        case 200, 202 -> accepted.incrementAndGet();
                        case 429 -> tooManyRequests.incrementAndGet();
                        case 503 -> unavailable.incrementAndGet();
                        default -> {
                            int failures = otherFailures.incrementAndGet();
                            if (failures <= 10) {
                                System.err.printf("%s HTTP failure status=%d body=%s%n",
                                        side, response.statusCode(), response.body());
                            }
                        }
                    }
                } catch (Exception e) {
                    int failures = otherFailures.incrementAndGet();
                    if (failures <= 10) {
                        System.err.printf("%s HTTP request failed: %s%n", side, e.getMessage());
                    }
                } finally {
                    inFlight.release();
                    done.countDown();
                }
            });
        }

        done.await();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        double elapsedSeconds = elapsedSince(startedAt);
        List<Long> sortedLatencies = new ArrayList<>(latenciesNanos);
        Collections.sort(sortedLatencies);
        return new HttpPhaseResult(
                accepted.get(),
                tooManyRequests.get(),
                unavailable.get(),
                otherFailures.get(),
                elapsedSeconds,
                percentileMillis(sortedLatencies, 0.50),
                percentileMillis(sortedLatencies, 0.95),
                percentileMillis(sortedLatencies, 0.99));
    }

    private static SellAdmissionResult waitForSellAdmission(
            Config config,
            DatabaseHandles databases,
            HttpClient httpClient,
            ObjectMapper objectMapper,
            List<UUID> sellOrderIds,
            long startedAtNanos) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.waitTimeoutSeconds());
        SellAdmissionResult latest = SellAdmissionResult.empty();
        long queueReadFailures = 0;
        long maxQueueBacklog = 0;
        int consecutiveDrainedSamples = 0;

        while (System.nanoTime() < deadline) {
            long submitted = countOrderEvents(
                    config, databases.order(), sellOrderIds, "OrderSubmissionRequestedV1");
            long reservationClaims = countWalletClaims(config, databases.wallet(), sellOrderIds);
            long confirmed = countOrderEvents(
                    config, databases.order(), sellOrderIds, "OrderAssetReservationConfirmedV1");
            long sellBook = redisInteger(config, "ZCARD", orderbookKey(config.marketId(), "sell"));
            QueueSnapshot queues = readQueues(config, httpClient, objectMapper);
            queueReadFailures += queues.readFailures();
            maxQueueBacklog = Math.max(maxQueueBacklog, queues.backlog());
            boolean drained = queues.backlog() == 0 && queues.readFailures() == 0;
            consecutiveDrainedSamples = drained ? consecutiveDrainedSamples + 1 : 0;
            latest = new SellAdmissionResult(
                    submitted,
                    reservationClaims,
                    confirmed,
                    sellBook,
                    queues.backlog(),
                    queueReadFailures,
                    maxQueueBacklog,
                    elapsedSince(startedAtNanos));
            if (submitted == config.events()
                    && reservationClaims == config.events()
                    && confirmed == config.events()
                    && sellBook == config.events()
                    && consecutiveDrainedSamples >= 3) {
                return latest;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        return latest;
    }

    private static CompletionResult waitForCompletion(
            Config config,
            DatabaseHandles databases,
            HttpClient httpClient,
            ObjectMapper objectMapper,
            List<UUID> buyOrderIds,
            List<UUID> sellOrderIds,
            List<UUID> allOrderIds,
            List<UUID> buyers,
            List<UUID> sellers,
            RoleBalances initialBuyerBalances,
            RoleBalances initialSellerBalances,
            long buyStartedAtNanos) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.waitTimeoutSeconds());
        long queueReadFailures = 0;
        long maxQueueBacklog = 0;
        int consecutiveDrainedSamples = 0;
        CompletionResult latest = CompletionResult.empty();

        while (System.nanoTime() < deadline) {
            long submitted = countOrderEvents(
                    config, databases.order(), allOrderIds, "OrderSubmissionRequestedV1");
            long reservationClaims = countWalletClaims(config, databases.wallet(), allOrderIds);
            long confirmed = countOrderEvents(
                    config, databases.order(), allOrderIds, "OrderAssetReservationConfirmedV1");
            long matchedOrders = countMatchedOrders(config, databases.order(), allOrderIds);
            long matchTrades = countMatchTrades(
                    config, databases.match(), config.marketId(), buyOrderIds, sellOrderIds);
            long orderTrades = countTradesByPrefix(
                    config,
                    databases.order(),
                    "SELECT count(*) FROM order_service.order_trade_applications WHERE left(trade_id, ?) = ?",
                    "SELECT count(*) FROM order_service.order_trade_applications",
                    config.marketId());
            long walletTrades = countTradesByPrefix(
                    config,
                    databases.wallet(),
                    "SELECT count(*) FROM wallet_service.trade_settlements WHERE left(trade_id, ?) = ?",
                    "SELECT count(*) FROM wallet_service.trade_settlements",
                    config.marketId());
            QueueSnapshot queues = readQueues(config, httpClient, objectMapper);
            queueReadFailures += queues.readFailures();
            maxQueueBacklog = Math.max(maxQueueBacklog, queues.backlog());

            RoleBalances buyerBalances = RoleBalances.empty();
            RoleBalances sellerBalances = RoleBalances.empty();
            long buyBook = -1;
            long sellBook = -1;
            long activeReservations = -1;
            TradeIdSetCheck tradeIds = TradeIdSetCheck.empty();
            boolean durableCountsReached = submitted == config.events() * 2L
                    && reservationClaims == config.events() * 2L
                    && confirmed == config.events() * 2L
                    && matchedOrders == config.events() * 2L
                    && matchTrades == config.events()
                    && orderTrades == config.events()
                    && walletTrades == config.events();
            if (durableCountsReached) {
                buyerBalances = readRoleBalances(databases.wallet(), buyers);
                sellerBalances = readRoleBalances(databases.wallet(), sellers);
                buyBook = redisInteger(config, "ZCARD", orderbookKey(config.marketId(), "buy"));
                sellBook = redisInteger(config, "ZCARD", orderbookKey(config.marketId(), "sell"));
                activeReservations = countRedisKeys(config, "order:reservation:*");
            }

            boolean queueDrained = queues.backlog() == 0 && queues.readFailures() == 0;
            consecutiveDrainedSamples = queueDrained ? consecutiveDrainedSamples + 1 : 0;
            boolean businessComplete = durableCountsReached
                    && balancesSettled(
                    config,
                    initialBuyerBalances,
                    initialSellerBalances,
                    buyerBalances,
                    sellerBalances)
                    && buyBook == 0
                    && sellBook == 0
                    && activeReservations == 0
                    && consecutiveDrainedSamples >= 3;
            double completionElapsedSeconds = elapsedSince(buyStartedAtNanos);
            double postCompletionVerificationSeconds = 0;
            if (businessComplete) {
                long verificationStartedAt = System.nanoTime();
                tradeIds = checkTradeIdSets(config, databases, buyOrderIds, sellOrderIds);
                postCompletionVerificationSeconds = elapsedSince(verificationStartedAt);
            }
            latest = new CompletionResult(
                    submitted,
                    reservationClaims,
                    confirmed,
                    matchedOrders,
                    matchTrades,
                    orderTrades,
                    walletTrades,
                    buyerBalances,
                    sellerBalances,
                    buyBook,
                    sellBook,
                    activeReservations,
                    tradeIds,
                    queues.backlog(),
                    queueReadFailures,
                    maxQueueBacklog,
                    queues.depths(),
                    completionElapsedSeconds,
                    postCompletionVerificationSeconds);

            if (businessComplete) {
                return latest;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        return latest;
    }

    private static boolean balancesSettled(
            Config config,
            RoleBalances initialBuyer,
            RoleBalances initialSeller,
            RoleBalances buyer,
            RoleBalances seller) {
        long tradedCurrency = config.events() * (long) PRICE * AMOUNT;
        long tradedAmount = config.events() * (long) AMOUNT;
        return buyer.availableAmount() == initialBuyer.availableAmount() + tradedAmount
                && buyer.availableCurrency() == initialBuyer.availableCurrency() - tradedCurrency
                && buyer.lockedAmount() == 0
                && buyer.lockedCurrency() == 0
                && seller.availableAmount() == initialSeller.availableAmount() - tradedAmount
                && seller.availableCurrency() == initialSeller.availableCurrency() + tradedCurrency
                && seller.lockedAmount() == 0
                && seller.lockedCurrency() == 0;
    }

    private static List<String> invalidReasons(
            Config config,
            HttpPhaseResult sellHttp,
            SellAdmissionResult sellAdmission,
            HttpPhaseResult buyHttp,
            CompletionResult completion,
            RoleBalances initialBuyerBalances,
            RoleBalances initialSellerBalances) {
        List<String> reasons = new ArrayList<>();
        addHttpInvalidReasons(reasons, "sell", config.events(), sellHttp);
        addHttpInvalidReasons(reasons, "buy", config.events(), buyHttp);
        if (sellAdmission.submitted() != config.events()) {
            reasons.add("sell_submission_count_mismatch");
        }
        if (sellAdmission.reservationClaims() != config.events()) {
            reasons.add("sell_wallet_reservation_count_mismatch");
        }
        if (sellAdmission.confirmed() != config.events()) {
            reasons.add("sell_confirmation_count_mismatch");
        }
        if (sellAdmission.sellBook() != config.events()) {
            reasons.add("sell_resting_book_count_mismatch");
        }
        if (sellAdmission.finalQueueBacklog() != 0 || sellAdmission.queueReadFailures() != 0) {
            reasons.add("sell_admission_queue_not_drained");
        }
        if (completion.submitted() != config.events() * 2L) {
            reasons.add("order_submission_count_mismatch");
        }
        if (completion.reservationClaims() != config.events() * 2L) {
            reasons.add("wallet_reservation_count_mismatch");
        }
        if (completion.confirmed() != config.events() * 2L) {
            reasons.add("order_confirmation_count_mismatch");
        }
        if (completion.matchedOrders() != config.events() * 2L) {
            reasons.add("order_matched_count_mismatch");
        }
        if (completion.matchTrades() != config.events()) {
            reasons.add("match_trade_count_mismatch");
        }
        if (completion.orderTrades() != config.events()) {
            reasons.add("order_trade_count_mismatch");
        }
        if (completion.walletTrades() != config.events()) {
            reasons.add("wallet_trade_count_mismatch");
        }
        if (!completion.tradeIds().equal()) {
            reasons.add("three_service_trade_id_set_mismatch");
        }
        if (!balancesSettled(
                config,
                initialBuyerBalances,
                initialSellerBalances,
                completion.buyerBalances(),
                completion.sellerBalances())) {
            reasons.add("asset_settlement_mismatch");
        }
        if (completion.buyBook() != 0 || completion.sellBook() != 0) {
            reasons.add("orderbook_not_empty");
        }
        if (completion.activeReservations() != 0) {
            reasons.add("match_reservations_not_drained");
        }
        if (completion.finalQueueBacklog() != 0) {
            reasons.add("final_queue_backlog");
        }
        if (completion.queueReadFailures() != 0) {
            reasons.add("queue_metrics_read_failures");
        }
        return reasons;
    }

    private static void addHttpInvalidReasons(
            List<String> reasons,
            String prefix,
            int expected,
            HttpPhaseResult result) {
        if (result.accepted() != expected) {
            reasons.add(prefix + "_http_accepted_mismatch");
        }
        if (result.tooManyRequests() != 0) {
            reasons.add(prefix + "_http_429");
        }
        if (result.unavailable() != 0) {
            reasons.add(prefix + "_http_503");
        }
        if (result.otherFailures() != 0) {
            reasons.add(prefix + "_http_other_failures");
        }
    }

    private static void printResult(
            Config config,
            HttpPhaseResult sellHttp,
            SellAdmissionResult sellAdmission,
            HttpPhaseResult buyHttp,
            CompletionResult completion,
            RoleBalances initialBuyerBalances,
            RoleBalances initialSellerBalances,
            double fullLifecycleSeconds,
            double benchmarkWallClockSeconds,
            List<String> invalidReasons) {
        System.out.println("{");
        System.out.printf("  \"benchmarkSchemaVersion\": %d,%n", BENCHMARK_SCHEMA_VERSION);
        System.out.println("  \"benchmarkContract\": \"http-matched-trade-completion-chain\",");
        System.out.printf("  \"runId\": \"%s\",%n", json(config.runId()));
        System.out.printf("  \"marketId\": \"%s\",%n", json(config.marketId()));
        System.out.printf("  \"expectedTrades\": %d,%n", config.events());
        System.out.printf("  \"offeredHttpOrders\": %d,%n", config.events() * 2L);
        System.out.printf("  \"targetOrderTpsPerPhase\": %d,%n", config.targetTps());
        printHttpPhase("sell", sellHttp);
        System.out.printf("  \"sellSubmissionRows\": %d,%n", sellAdmission.submitted());
        System.out.printf("  \"sellWalletReservationRows\": %d,%n", sellAdmission.reservationClaims());
        System.out.printf("  \"sellConfirmationRows\": %d,%n", sellAdmission.confirmed());
        System.out.printf("  \"sellRestingBookCount\": %d,%n", sellAdmission.sellBook());
        System.out.printf("  \"sellAdmissionSeconds\": %.4f,%n", sellAdmission.elapsedSeconds());
        System.out.printf("  \"sellAdmissionQueueBacklog\": %d,%n", sellAdmission.finalQueueBacklog());
        System.out.printf("  \"sellAdmissionMaxQueueBacklog\": %d,%n", sellAdmission.maxQueueBacklog());
        System.out.printf("  \"sellAdmissionQueueReadFailures\": %d,%n", sellAdmission.queueReadFailures());
        printHttpPhase("buy", buyHttp);
        System.out.printf("  \"orderSubmissionRows\": %d,%n", completion.submitted());
        System.out.printf("  \"walletReservationRows\": %d,%n", completion.reservationClaims());
        System.out.printf("  \"orderConfirmationRows\": %d,%n", completion.confirmed());
        System.out.printf("  \"orderMatchedRows\": %d,%n", completion.matchedOrders());
        System.out.printf("  \"matchTradeRows\": %d,%n", completion.matchTrades());
        System.out.printf("  \"orderTradeRows\": %d,%n", completion.orderTrades());
        System.out.printf("  \"walletTradeRows\": %d,%n", completion.walletTrades());
        System.out.printf("  \"threeServiceTradeIdsEqual\": %s,%n", completion.tradeIds().equal());
        System.out.printf("  \"matchTradeIdCount\": %d,%n", completion.tradeIds().matchCount());
        System.out.printf("  \"orderTradeIdCount\": %d,%n", completion.tradeIds().orderCount());
        System.out.printf("  \"walletTradeIdCount\": %d,%n", completion.tradeIds().walletCount());
        System.out.printf("  \"tradeIdUnionCount\": %d,%n", completion.tradeIds().unionCount());
        System.out.printf("  \"tradeIdFingerprint\": \"%s\",%n", completion.tradeIds().fingerprint());
        printBalances("initialBuyer", initialBuyerBalances);
        printBalances("initialSeller", initialSellerBalances);
        printBalances("finalBuyer", completion.buyerBalances());
        printBalances("finalSeller", completion.sellerBalances());
        System.out.printf("  \"remainingBuyOrders\": %d,%n", completion.buyBook());
        System.out.printf("  \"remainingSellOrders\": %d,%n", completion.sellBook());
        System.out.printf("  \"activeMatchReservations\": %d,%n", completion.activeReservations());
        System.out.printf("  \"finalQueueBacklog\": %d,%n", completion.finalQueueBacklog());
        System.out.printf("  \"maxQueueBacklog\": %d,%n", completion.maxQueueBacklog());
        System.out.printf("  \"queueMetricsReadFailures\": %d,%n", completion.queueReadFailures());
        printQueueDepths(completion.finalQueueDepths());
        System.out.printf("  \"buyTriggeredCompletionSeconds\": %.4f,%n", completion.elapsedSeconds());
        System.out.printf("  \"buyTriggeredTradeCompletionTps\": %.2f,%n",
                config.events() / Math.max(completion.elapsedSeconds(), 0.001));
        System.out.printf("  \"fullHttpLifecycleSeconds\": %.4f,%n", fullLifecycleSeconds);
        System.out.printf("  \"businessHttpMatchedTradeCompletionTps\": %.2f,%n",
                config.events() / Math.max(fullLifecycleSeconds, 0.001));
        System.out.printf("  \"businessHttpOrderConvergenceTps\": %.2f,%n",
                config.events() * 2.0 / Math.max(fullLifecycleSeconds, 0.001));
        System.out.printf("  \"postCompletionVerificationSeconds\": %.4f,%n",
                completion.postCompletionVerificationSeconds());
        System.out.printf("  \"benchmarkWallClockSeconds\": %.4f,%n", benchmarkWallClockSeconds);
        System.out.println("  \"completionTimingExcludesVerification\": true,");
        System.out.printf("  \"validForCapacityComparison\": %s,%n", invalidReasons.isEmpty());
        System.out.printf("  \"capacityInvalidReasons\": %s%n", jsonArray(invalidReasons));
        System.out.println("}");
    }

    private static void printHttpPhase(String prefix, HttpPhaseResult result) {
        System.out.printf("  \"%sHttpAccepted\": %d,%n", prefix, result.accepted());
        System.out.printf("  \"%sHttp429\": %d,%n", prefix, result.tooManyRequests());
        System.out.printf("  \"%sHttp503\": %d,%n", prefix, result.unavailable());
        System.out.printf("  \"%sHttpOtherFailures\": %d,%n", prefix, result.otherFailures());
        System.out.printf("  \"%sHttpSendSeconds\": %.4f,%n", prefix, result.elapsedSeconds());
        System.out.printf("  \"%sHttpAcceptedTps\": %.2f,%n",
                prefix, result.accepted() / Math.max(result.elapsedSeconds(), 0.001));
        System.out.printf("  \"%sHttpP50Ms\": %.2f,%n", prefix, result.p50Millis());
        System.out.printf("  \"%sHttpP95Ms\": %.2f,%n", prefix, result.p95Millis());
        System.out.printf("  \"%sHttpP99Ms\": %.2f,%n", prefix, result.p99Millis());
    }

    private static void printBalances(String prefix, RoleBalances balances) {
        System.out.printf("  \"%sAvailableAmount\": %d,%n", prefix, balances.availableAmount());
        System.out.printf("  \"%sLockedAmount\": %d,%n", prefix, balances.lockedAmount());
        System.out.printf("  \"%sAvailableCurrency\": %d,%n", prefix, balances.availableCurrency());
        System.out.printf("  \"%sLockedCurrency\": %d,%n", prefix, balances.lockedCurrency());
    }

    private static void printQueueDepths(Map<String, QueueDepth> depths) {
        System.out.println("  \"finalQueues\": {");
        for (int i = 0; i < CHAIN_QUEUES.size(); i++) {
            String queue = CHAIN_QUEUES.get(i);
            QueueDepth depth = depths.getOrDefault(queue, new QueueDepth(-1, -1));
            System.out.printf("    \"%s\": {\"ready\": %d, \"unacked\": %d}%s%n",
                    json(queue), depth.ready(), depth.unacked(), i + 1 < CHAIN_QUEUES.size() ? "," : "");
        }
        System.out.println("  },");
    }

    private static List<UUID> registerUsers(
            Config config,
            HttpClient httpClient,
            ObjectMapper objectMapper,
            int count) throws Exception {
        List<UUID> users = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(config.walletUrl() + "/v1/wallet/register"))
                    .timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException(
                        "wallet registration failed: status=" + response.statusCode() + ", body=" + response.body());
            }
            JsonNode body = objectMapper.readTree(response.body());
            users.add(UUID.fromString(body.path("userId").asText()));
        }
        return users;
    }

    private static void reset(Config config, HttpClient httpClient, ObjectMapper objectMapper) throws Exception {
        System.out.println("resetting HTTP matched-trade completion benchmark data");
        RabbitManagementClient rabbit = rabbitManagementClient(config, httpClient, objectMapper);
        rabbit.purgeQueues(CHAIN_QUEUES);
        truncateAllServiceData(config);
        awaitResetQueueQuiescence(rabbit, 30);
        truncateAllServiceData(config);
        rabbit.purgeQueues(CHAIN_QUEUES);
        if (config.flushRedisOnReset()) {
            redisCommand(config, "FLUSHDB");
        } else {
            redisCommand(
                    config,
                    "DEL",
                    orderbookKey(config.marketId(), "buy"),
                    orderbookKey(config.marketId(), "sell"));
            deleteRedisKeys(config, "order:reservation:*");
            deleteRedisKeys(config, "order:cancellation:*");
            deleteRedisKeys(config, "order:cancellation-intent:*");
        }
        awaitResetQueueQuiescence(rabbit, 10);
    }

    private static void truncateAllServiceData(Config config) throws Exception {
        truncateOrderData(config);
        truncateWalletData(config);
        truncateMatchData(config);
    }

    private static void awaitResetQueueQuiescence(
            RabbitManagementClient rabbit,
            int timeoutSeconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        int consecutiveEmptySamples = 0;
        QueueSnapshot latest = new QueueSnapshot(Long.MAX_VALUE, CHAIN_QUEUES.size(), Map.of());
        while (System.nanoTime() < deadline) {
            rabbit.purgeQueues(CHAIN_QUEUES);
            latest = rabbit.readQueuesAllowMissing(CHAIN_QUEUES);
            if (latest.backlog() == 0 && latest.readFailures() == 0) {
                consecutiveEmptySamples++;
                if (consecutiveEmptySamples >= 3) {
                    return;
                }
            } else {
                consecutiveEmptySamples = 0;
            }
            TimeUnit.MILLISECONDS.sleep(200);
        }
        throw new IllegalStateException("queues did not quiesce during reset: " + latest);
    }

    private static void truncateOrderData(Config config) throws Exception {
        execute(config.orderJdbcUrl(), config.jdbcUser(), config.jdbcPassword(), """
                DO $$
                BEGIN
                    IF to_regclass('order_service.order_cancellation_result_inbox') IS NOT NULL THEN
                        TRUNCATE TABLE order_service.order_cancellation_result_inbox;
                    END IF;
                END $$;
                TRUNCATE TABLE
                    order_service.match_history,
                    order_service.order_trade_execution_inbox,
                    order_service.order_trade_applications,
                    order_service.order_event_store_relay_checkpoints,
                    order_service.order_event_outbox,
                    order_service.order_matching_state,
                    order_service.orders_current,
                    order_service.projection_checkpoints,
                    order_service.order_event_store,
                    order_service.order_stream_heads
                RESTART IDENTITY CASCADE
                """);
    }

    private static void truncateWalletData(Config config) throws Exception {
        execute(config.walletJdbcUrl(), config.jdbcUser(), config.jdbcPassword(), """
                DO $$
                BEGIN
                    IF to_regclass('wallet_service.order_cancellation_applications') IS NOT NULL THEN
                        TRUNCATE TABLE wallet_service.order_cancellation_applications;
                    END IF;
                END $$;
                TRUNCATE TABLE
                    wallet_service.trade_settlements,
                    wallet_service.outbox,
                    wallet_service.order_submission_idempotency,
                    wallet_service.settlement_idempotency,
                    wallet_service.wallets
                RESTART IDENTITY CASCADE
                """);
    }

    private static void truncateMatchData(Config config) throws Exception {
        execute(config.matchJdbcUrl(), config.jdbcUser(), config.jdbcPassword(), """
                DO $$
                BEGIN
                    IF to_regclass('match_engine.trade_publish_checkpoints') IS NOT NULL THEN
                        TRUNCATE TABLE match_engine.trade_publish_checkpoints RESTART IDENTITY CASCADE;
                    END IF;
                    IF to_regclass('match_engine.reservation_cleanup_tasks') IS NOT NULL THEN
                        TRUNCATE TABLE match_engine.reservation_cleanup_tasks RESTART IDENTITY CASCADE;
                    END IF;
                    IF to_regclass('match_engine.order_cancellations') IS NOT NULL THEN
                        TRUNCATE TABLE match_engine.order_cancellations RESTART IDENTITY CASCADE;
                    END IF;
                END $$;
                TRUNCATE TABLE
                    match_engine.trade_outbox,
                    match_engine.trade_executions
                RESTART IDENTITY CASCADE;
                """);
    }

    private static void execute(String jdbcUrl, String user, String password, String sql) throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
    }

    private static long countOrderEvents(
            Config config,
            Connection connection,
            List<UUID> orderIds,
            String eventType)
            throws Exception {
        if (config.resetData()) {
            try (PreparedStatement statement = connection.prepareStatement("""
                    SELECT count(*)
                    FROM order_service.order_event_store
                    WHERE event_type = ?
                    """)) {
                statement.setString(1, eventType);
                try (ResultSet resultSet = statement.executeQuery()) {
                    return resultSet.next() ? resultSet.getLong(1) : 0;
                }
            }
        }
        return queryWithUuidArray(
                connection,
                """
                        SELECT count(*)
                        FROM order_service.order_event_store
                        WHERE event_type = ?
                          AND aggregate_id = ANY(?)
                        """,
                eventType,
                orderIds);
    }

    private static long countWalletClaims(
            Config config,
            Connection connection,
            List<UUID> orderIds) throws Exception {
        if (config.resetData()) {
            return queryScalarLong(
                    connection,
                    "SELECT count(*) FROM wallet_service.order_submission_idempotency");
        }
        return queryWithUuidArray(
                connection,
                """
                        SELECT count(*)
                        FROM wallet_service.order_submission_idempotency
                        WHERE order_id = ANY(?)
                        """,
                null,
                orderIds);
    }

    private static long countMatchedOrders(
            Config config,
            Connection connection,
            List<UUID> orderIds) throws Exception {
        if (config.resetData()) {
            return queryScalarLong(
                    connection,
                    """
                            SELECT count(*)
                            FROM order_service.order_matching_state
                            WHERE status = 'MATCHED'
                              AND remaining_amount = 0
                            """);
        }
        return queryWithUuidArray(
                connection,
                """
                        SELECT count(*)
                        FROM order_service.order_matching_state
                        WHERE order_id = ANY(?)
                          AND status = 'MATCHED'
                          AND remaining_amount = 0
                        """,
                null,
                orderIds);
    }

    private static long queryWithUuidArray(
            Connection connection,
            String sql,
            String leadingString,
            List<UUID> ids) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int arrayIndex = 1;
            if (leadingString != null) {
                statement.setString(1, leadingString);
                arrayIndex = 2;
            }
            Array sqlArray = connection.createArrayOf("uuid", ids.toArray(UUID[]::new));
            statement.setArray(arrayIndex, sqlArray);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getLong(1) : 0;
            } finally {
                sqlArray.free();
            }
        }
    }

    private static long countMatchTrades(
            Config config,
            Connection connection,
            String marketId,
            List<UUID> buyOrderIds,
            List<UUID> sellOrderIds) throws Exception {
        if (config.resetData()) {
            try (PreparedStatement statement = connection.prepareStatement("""
                    SELECT count(*)
                    FROM match_engine.trade_executions
                    WHERE market_id = ?
                    """)) {
                statement.setString(1, marketId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    return resultSet.next() ? resultSet.getLong(1) : 0;
                }
            }
        }
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT count(*)
                FROM match_engine.trade_executions
                WHERE market_id = ?
                  AND buyer_order_id = ANY(?)
                  AND seller_order_id = ANY(?)
                """)) {
            statement.setString(1, marketId);
            Array buyArray = connection.createArrayOf("uuid", buyOrderIds.toArray(UUID[]::new));
            Array sellArray = connection.createArrayOf("uuid", sellOrderIds.toArray(UUID[]::new));
            statement.setArray(2, buyArray);
            statement.setArray(3, sellArray);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getLong(1) : 0;
            } finally {
                buyArray.free();
                sellArray.free();
            }
        }
    }

    private static long countTradesByPrefix(
            Config config,
            Connection connection,
            String scopedSql,
            String resetDataSql,
            String marketId)
            throws Exception {
        if (config.resetData()) {
            return queryScalarLong(connection, resetDataSql);
        }
        String prefix = marketId + "-";
        try (PreparedStatement statement = connection.prepareStatement(scopedSql)) {
            statement.setInt(1, prefix.length());
            statement.setString(2, prefix);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getLong(1) : 0;
            }
        }
    }

    private static long countTradesByPrefix(Connection connection, String sql, String marketId)
            throws Exception {
        String prefix = marketId + "-";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, prefix.length());
            statement.setString(2, prefix);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getLong(1) : 0;
            }
        }
    }

    private static long queryScalarLong(Connection connection, String sql) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next() ? resultSet.getLong(1) : 0;
        }
    }

    private static RoleBalances readRoleBalances(Connection connection, List<UUID> users) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT
                    COALESCE(sum(available_amount), 0),
                    COALESCE(sum(locked_amount), 0),
                    COALESCE(sum(available_currency), 0),
                    COALESCE(sum(locked_currency), 0)
                FROM wallet_service.wallets
                WHERE user_id = ANY(?)
                """)) {
            Array sqlArray = connection.createArrayOf("uuid", users.toArray(UUID[]::new));
            statement.setArray(1, sqlArray);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return RoleBalances.empty();
                }
                return new RoleBalances(
                        resultSet.getLong(1),
                        resultSet.getLong(2),
                        resultSet.getLong(3),
                        resultSet.getLong(4));
            } finally {
                sqlArray.free();
            }
        }
    }

    private static TradeIdSetCheck checkTradeIdSets(
            Config config,
            DatabaseHandles databases,
            List<UUID> buyOrderIds,
            List<UUID> sellOrderIds) throws Exception {
        Set<String> matchIds = queryMatchTradeIds(
                databases.match(), config.marketId(), buyOrderIds, sellOrderIds);
        Set<String> orderIds = queryTradeIdsByPrefix(
                databases.order(),
                "SELECT trade_id FROM order_service.order_trade_applications WHERE left(trade_id, ?) = ?",
                config.marketId());
        Set<String> walletIds = queryTradeIdsByPrefix(
                databases.wallet(),
                "SELECT trade_id FROM wallet_service.trade_settlements WHERE left(trade_id, ?) = ?",
                config.marketId());
        Set<String> union = new HashSet<>(matchIds);
        union.addAll(orderIds);
        union.addAll(walletIds);
        return new TradeIdSetCheck(
                matchIds.equals(orderIds)
                        && matchIds.equals(walletIds)
                        && matchIds.size() == config.events(),
                matchIds.size(),
                orderIds.size(),
                walletIds.size(),
                union.size(),
                fingerprint(union));
    }

    private static Set<String> queryMatchTradeIds(
            Connection connection,
            String marketId,
            List<UUID> buyOrderIds,
            List<UUID> sellOrderIds) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT trade_id
                FROM match_engine.trade_executions
                WHERE market_id = ?
                  AND buyer_order_id = ANY(?)
                  AND seller_order_id = ANY(?)
                """)) {
            statement.setString(1, marketId);
            Array buyArray = connection.createArrayOf("uuid", buyOrderIds.toArray(UUID[]::new));
            Array sellArray = connection.createArrayOf("uuid", sellOrderIds.toArray(UUID[]::new));
            statement.setArray(2, buyArray);
            statement.setArray(3, sellArray);
            try (ResultSet resultSet = statement.executeQuery()) {
                Set<String> result = new HashSet<>();
                while (resultSet.next()) {
                    result.add(resultSet.getString(1));
                }
                return result;
            } finally {
                buyArray.free();
                sellArray.free();
            }
        }
    }

    private static Set<String> queryTradeIdsByPrefix(
            Connection connection,
            String sql,
            String marketId) throws Exception {
        String prefix = marketId + "-";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, prefix.length());
            statement.setString(2, prefix);
            try (ResultSet resultSet = statement.executeQuery()) {
                Set<String> result = new HashSet<>();
                while (resultSet.next()) {
                    result.add(resultSet.getString(1));
                }
                return result;
            }
        }
    }

    private static QueueSnapshot readQueues(
            Config config,
            HttpClient httpClient,
            ObjectMapper objectMapper) {
        return rabbitManagementClient(config, httpClient, objectMapper).readQueues(CHAIN_QUEUES);
    }

    private static void purgeQueues(Config config, HttpClient httpClient) throws Exception {
        rabbitManagementClient(config, httpClient, new ObjectMapper()).purgeQueues(CHAIN_QUEUES);
    }

    private static RabbitManagementClient rabbitManagementClient(
            Config config,
            HttpClient httpClient,
            ObjectMapper objectMapper) {
        return new RabbitManagementClient(
                config.rabbitManagementUrl(),
                config.rabbitVhost(),
                config.rabbitUser(),
                config.rabbitPassword(),
                httpClient,
                objectMapper);
    }

    private static long redisInteger(Config config, String command, String... args) throws IOException {
        return redisClient(config).integer(command, args);
    }

    private static Object redisCommand(Config config, String command, String... args) throws IOException {
        return redisClient(config).command(command, args);
    }

    private static long countRedisKeys(Config config, String pattern) throws IOException {
        return redisClient(config).countKeys(pattern);
    }

    private static void deleteRedisKeys(Config config, String pattern) throws IOException {
        redisClient(config).deleteKeys(pattern);
    }

    private static RedisRespClient redisClient(Config config) {
        return new RedisRespClient(config.redisHost(), config.redisPort());
    }

    private static List<UUID> orderIds(String runId, String side, int events) {
        List<UUID> ids = new ArrayList<>(events);
        for (int i = 0; i < events; i++) {
            ids.add(UUID.nameUUIDFromBytes(
                    (runId + ":" + side + ":" + i).getBytes(StandardCharsets.UTF_8)));
        }
        return ids;
    }

    private static String fingerprint(Set<String> tradeIds) {
        List<String> sorted = new ArrayList<>(tradeIds);
        sorted.sort(String::compareTo);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String tradeId : sorted) {
                digest.update(tradeId.getBytes(StandardCharsets.UTF_8));
                digest.update((byte) 0);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static String orderbookKey(String marketId, String side) {
        return "orderbook:" + marketId + ":" + side;
    }

    private static void throttle(AtomicLong nextSendAtNanos, long intervalNanos) {
        long scheduledAt = nextSendAtNanos.getAndAdd(intervalNanos);
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

    private static double elapsedSince(long startedAtNanos) {
        return (System.nanoTime() - startedAtNanos) / 1_000_000_000.0;
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
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        values.forEach(value -> joiner.add("\"" + json(value) + "\""));
        return joiner.toString();
    }

    static long runDelta(long current, long baseline, String factName) {
        long delta = current - baseline;
        if (delta < 0) {
            throw new IllegalStateException(
                    "Durable " + factName + " count moved below the run baseline: current="
                            + current + ", baseline=" + baseline);
        }
        return delta;
    }

    static boolean exceedsMeaningfulBacklogGrowth(
            double backlogSlopePerSecond,
            long startBacklog,
            long endBacklog,
            double observedSeconds,
            int targetOrderTps,
            double maxBacklogGrowthPerSecond) {
        double netGrowthPerSecond =
                (endBacklog - startBacklog) / Math.max(observedSeconds, 0.001);
        long minimumMeaningfulGrowth = Math.max(
                targetOrderTps,
                (long) Math.ceil(maxBacklogGrowthPerSecond * observedSeconds));
        return backlogSlopePerSecond > maxBacklogGrowthPerSecond
                && netGrowthPerSecond > maxBacklogGrowthPerSecond
                && endBacklog - startBacklog > minimumMeaningfulGrowth;
    }

    static BacklogWindow summarizeBacklog(List<SteadySample> samples) {
        List<SteadySample> validSamples = samples.stream()
                .filter(sample -> sample.queueReadFailures() == 0)
                .toList();
        if (validSamples.isEmpty()) {
            return new BacklogWindow(-1, -1, -1, 0, 0);
        }
        SteadySample first = validSamples.get(0);
        SteadySample last = validSamples.get(validSamples.size() - 1);
        long max = validSamples.stream()
                .mapToLong(SteadySample::queueBacklog)
                .max()
                .orElse(-1);
        return new BacklogWindow(
                first.queueBacklog(),
                last.queueBacklog(),
                max,
                backlogSlope(validSamples),
                validSamples.size());
    }

    private static double backlogSlope(List<SteadySample> samples) {
        if (samples.size() < 2) {
            return 0;
        }
        double origin = samples.get(0).elapsedSeconds();
        double sumX = 0;
        double sumY = 0;
        double sumXX = 0;
        double sumXY = 0;
        for (SteadySample sample : samples) {
            double x = sample.elapsedSeconds() - origin;
            double y = sample.queueBacklog();
            sumX += x;
            sumY += y;
            sumXX += x * x;
            sumXY += x * y;
        }
        double count = samples.size();
        double denominator = count * sumXX - sumX * sumX;
        return Math.abs(denominator) < 0.000001
                ? 0
                : (count * sumXY - sumX * sumY) / denominator;
    }

    private static final class SteadyStateRunner {

        private final SteadyStateConfig config;
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        private SteadyStateRunner(SteadyStateConfig config) {
            this.config = config;
        }

        private void run() throws Exception {
            Config common = config.common();
            if (!HTTP_MARKET_ID.equals(common.marketId())) {
                throw new IllegalArgumentException(
                        "Order HTTP currently assigns market " + HTTP_MARKET_ID + "; --market-id must match");
            }
            if (common.resetData()) {
                reset(common, httpClient, objectMapper);
            }

            System.out.printf(
                    "registering %d buyers and %d sellers for steady-state HTTP traffic%n",
                    common.usersPerSide(), common.usersPerSide());
            List<UUID> buyers = registerUsers(common, httpClient, objectMapper, common.usersPerSide());
            List<UUID> sellers = registerUsers(common, httpClient, objectMapper, common.usersPerSide());
            List<UUID> allUsers = new ArrayList<>(buyers.size() + sellers.size());
            allUsers.addAll(buyers);
            allUsers.addAll(sellers);

            try (DatabaseHandles databases = DatabaseHandles.open(common)) {
                fundSteadyStateUsers(databases.wallet(), allUsers, common.events());
                RoleBalances initialBuyerBalances = readRoleBalances(databases.wallet(), buyers);
                RoleBalances initialSellerBalances = readRoleBalances(databases.wallet(), sellers);
                long submissionBaselinePosition = queryLong(
                        databases.order(),
                        "SELECT COALESCE(max(global_position), 0) FROM order_service.order_event_store");
                SteadyDatabaseBaseline databaseBaseline = captureDatabaseBaseline(databases, common);

                List<PreparedHttpLoadDriver.PreparedOrder> preparedOrders = List.of();
                double preparationSeconds = 0;
                if (config.usesPreparedDriver()) {
                    long preparationStartedAtNanos = System.nanoTime();
                    preparedOrders = prepareSteadyOrders(
                            buyers,
                            sellers,
                            0,
                            common.events(),
                            config.arrivalPattern(),
                            config.workloadSeed());
                    preparationSeconds = elapsedSince(preparationStartedAtNanos);
                }
                System.out.printf(
                        "prepared steady HTTP workload: orders=%d, driver=%s, preparationSeconds=%.4f%n",
                        config.usesPreparedDriver() ? preparedOrders.size() : 0,
                        config.httpDriverMode(),
                        preparationSeconds);

                SteadyHttpCounters counters = new SteadyHttpCounters();
                List<SteadySample> samples = Collections.synchronizedList(new ArrayList<>());
                AtomicBoolean monitorRunning = new AtomicBoolean(true);
                long startedAtNanos = System.nanoTime();
                ExecutorService monitorExecutor = Executors.newSingleThreadExecutor();
                Future<?> monitor = monitorExecutor.submit(() ->
                        monitor(samples, counters, monitorRunning, startedAtNanos, databaseBaseline));

                double sendElapsedSeconds;
                try {
                    sendContinuousTraffic(
                            buyers, sellers, preparedOrders, counters, startedAtNanos);
                    sendElapsedSeconds = elapsedSince(startedAtNanos);
                } finally {
                    monitorRunning.set(false);
                    monitor.get();
                    monitorExecutor.shutdown();
                    monitorExecutor.awaitTermination(10, TimeUnit.SECONDS);
                }

                SteadyWindow steadyWindow = deriveSteadyWindow(samples);
                SteadyExpectedOutcome expectedOutcome = resolveExpectedOutcome(
                        databases.order(),
                        common,
                        counters,
                        submissionBaselinePosition);
                SteadyCompletion completion = waitForSteadyCompletion(
                        databases,
                        buyers,
                        sellers,
                        initialBuyerBalances,
                        initialSellerBalances,
                        startedAtNanos,
                        common,
                        expectedOutcome,
                        databaseBaseline);
                double fullConvergenceSeconds = elapsedSince(startedAtNanos);
                writeSamples(samples);

                List<String> invalidReasons = steadyInvalidReasons(
                        counters,
                        steadyWindow,
                        completion,
                        initialBuyerBalances,
                        initialSellerBalances,
                        expectedOutcome);
                printSteadyResult(
                        counters,
                        steadyWindow,
                        completion,
                        initialBuyerBalances,
                        initialSellerBalances,
                        preparationSeconds,
                        sendElapsedSeconds,
                        fullConvergenceSeconds,
                        expectedOutcome,
                        invalidReasons);
                if (!invalidReasons.isEmpty()) {
                    throw new IllegalStateException(
                            "HTTP matched steady-state benchmark invalid: " + invalidReasons);
                }
            }
        }

        private void fundSteadyStateUsers(
                Connection wallet,
                List<UUID> users,
                long expectedTrades) throws Exception {
            long tradesPerUser = (expectedTrades + config.common().usersPerSide() - 1)
                    / config.common().usersPerSide();
            long fundedAmount = Math.max(10_000L, tradesPerUser * AMOUNT + 10_000L);
            long fundedCurrency = Math.max(10_000L, tradesPerUser * PRICE * AMOUNT + 1_000_000L);
            if (fundedAmount > Integer.MAX_VALUE || fundedCurrency > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("steady-state funding exceeds Wallet integer capacity");
            }
            try (PreparedStatement statement = wallet.prepareStatement("""
                    UPDATE wallet_service.wallets
                    SET available_amount = ?,
                        available_currency = ?,
                        locked_amount = 0,
                        locked_currency = 0,
                        version = version + 1,
                        update_time = CURRENT_TIMESTAMP
                    WHERE user_id = ANY(?)
                    """)) {
                statement.setInt(1, (int) fundedAmount);
                statement.setInt(2, (int) fundedCurrency);
                Array userArray = wallet.createArrayOf("uuid", users.toArray(UUID[]::new));
                statement.setArray(3, userArray);
                try {
                    int updated = statement.executeUpdate();
                    if (updated != users.size()) {
                        throw new IllegalStateException(
                                "steady-state wallet funding mismatch: expected=" + users.size() + ", updated=" + updated);
                    }
                } finally {
                    userArray.free();
                }
            }
            System.out.printf(
                    "funded steady-state wallets: users=%d, amountPerUser=%d, currencyPerUser=%d%n",
                    users.size(), fundedAmount, fundedCurrency);
        }

        private void sendContinuousTraffic(
                List<UUID> buyers,
                List<UUID> sellers,
                List<PreparedHttpLoadDriver.PreparedOrder> preparedOrders,
                SteadyHttpCounters counters,
                long startedAtNanos) throws Exception {
            Config common = config.common();
            int totalOrders = Math.multiplyExact(common.events(), 2);
            long schedulingDeadlineNanos = startedAtNanos
                    + TimeUnit.SECONDS.toNanos(
                            config.warmupSeconds()
                                    + config.durationSeconds()
                                    + MAX_STEADY_SCHEDULING_OVERRUN_SECONDS);

            System.out.printf(
                    "sending continuous balanced mixed HTTP traffic: totalOrders=%d, expectedTrades=%d, "
                            + "targetTotalOrderTps=%d, warmupSeconds=%d, measurementSeconds=%d, "
                            + "arrivalPattern=%s, workloadSeed=%d, driver=%s, maxInFlight=%d%n",
                    totalOrders,
                    common.events(),
                    config.targetOrderTps(),
                    config.warmupSeconds(),
                    config.durationSeconds(),
                    config.arrivalPattern().externalName(),
                    config.workloadSeed(),
                    config.httpDriverMode(),
                    common.maxInFlight());

            int dispatchedOrders;
            int unscheduledOrders;
            if (config.usesPreparedDriver()) {
                PreparedHttpLoadDriver.ReplayResult replay = replayPreparedOrders(
                        preparedOrders,
                        counters,
                        null,
                        config.targetOrderTps(),
                        common.maxInFlight(),
                        startedAtNanos,
                        schedulingDeadlineNanos);
                dispatchedOrders = replay.dispatchedOrders();
                unscheduledOrders = replay.unscheduledOrders();
            } else {
                dispatchedOrders = sendLegacyContinuousTraffic(
                        buyers,
                        sellers,
                        counters,
                        startedAtNanos,
                        schedulingDeadlineNanos);
                unscheduledOrders = totalOrders - dispatchedOrders;
            }
            if (unscheduledOrders > 0) {
                counters.recordUnscheduled(unscheduledOrders);
                System.err.printf(
                        "steady scheduling deadline exceeded: submitted=%d, unscheduled=%d, graceSeconds=%d%n",
                        dispatchedOrders,
                        unscheduledOrders,
                        MAX_STEADY_SCHEDULING_OVERRUN_SECONDS);
            }
        }

        private int sendLegacyContinuousTraffic(
                List<UUID> buyers,
                List<UUID> sellers,
                SteadyHttpCounters counters,
                long startedAtNanos,
                long schedulingDeadlineNanos) throws InterruptedException {
            Config common = config.common();
            int totalOrders = Math.multiplyExact(common.events(), 2);
            CountDownLatch done = new CountDownLatch(totalOrders);
            Semaphore inFlight = new Semaphore(common.maxInFlight());
            ExecutorService executor = Executors.newFixedThreadPool(common.workers());
            AtomicLong nextSendAtNanos = new AtomicLong(startedAtNanos);
            long orderIntervalNanos = TimeUnit.SECONDS.toNanos(1) / config.targetOrderTps();
            List<BalancedOrderSchedule.ScheduledOrder> schedule = BalancedOrderSchedule.create(
                    0,
                    common.events(),
                    config.arrivalPattern(),
                    config.workloadSeed());
            int dispatchedOrders = 0;
            for (BalancedOrderSchedule.ScheduledOrder scheduledOrder : schedule) {
                if (System.nanoTime() >= schedulingDeadlineNanos) {
                    break;
                }
                String side = scheduledOrder.side();
                List<UUID> users = "BUY".equals(side) ? buyers : sellers;
                throttle(nextSendAtNanos, orderIntervalNanos);
                if (System.nanoTime() >= schedulingDeadlineNanos) {
                    break;
                }
                submitLegacySteadyOrder(
                        executor,
                        inFlight,
                        done,
                        counters,
                        null,
                        side,
                        deterministicSteadyOrderId(
                                common.runId(), side, scheduledOrder.tradeIndex()),
                        users.get(scheduledOrder.userSequence() % users.size()));
                dispatchedOrders++;
            }
            for (int index = dispatchedOrders; index < totalOrders; index++) {
                done.countDown();
            }
            done.await();
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            return dispatchedOrders;
        }

        private List<PreparedHttpLoadDriver.PreparedOrder> prepareSteadyOrders(
                List<UUID> buyers,
                List<UUID> sellers,
                int startingTradeIndex,
                int trades,
                BalancedOrderSchedule.ArrivalPattern arrivalPattern,
                long workloadSeed) throws IOException {
            Config common = config.common();
            URI buyUri = URI.create(common.orderUrl() + "/bid/buy");
            URI sellUri = URI.create(common.orderUrl() + "/bid/sell");
            List<BalancedOrderSchedule.ScheduledOrder> schedule = BalancedOrderSchedule.create(
                    startingTradeIndex,
                    trades,
                    arrivalPattern,
                    workloadSeed);
            List<PreparedHttpLoadDriver.PreparedOrder> prepared =
                    new ArrayList<>(schedule.size());
            for (BalancedOrderSchedule.ScheduledOrder scheduledOrder : schedule) {
                String side = scheduledOrder.side();
                boolean buy = "BUY".equals(side);
                List<UUID> users = buy ? buyers : sellers;
                UUID userId = users.get(scheduledOrder.userSequence() % users.size());
                UUID orderId = deterministicSteadyOrderId(
                        common.runId(), side, scheduledOrder.tradeIndex());
                byte[] body = buy
                        ? objectMapper.writeValueAsBytes(new BuyRequest(orderId, PRICE, AMOUNT, userId))
                        : objectMapper.writeValueAsBytes(new SellRequest(orderId, PRICE, AMOUNT, userId));
                prepared.add(new PreparedHttpLoadDriver.PreparedOrder(
                        side,
                        buy ? buyUri : sellUri,
                        body));
            }
            return List.copyOf(prepared);
        }

        private PreparedHttpLoadDriver.ReplayResult replayPreparedOrders(
                List<PreparedHttpLoadDriver.PreparedOrder> preparedOrders,
                SteadyHttpCounters counters,
                SteadyHttpCounters secondaryCounters,
                int targetOrderTps,
                int maxInFlight,
                long firstSendAtNanos,
                long schedulingDeadlineNanos) throws InterruptedException {
            return PreparedHttpLoadDriver.replay(
                    httpClient,
                    preparedOrders,
                    targetOrderTps,
                    config.common().workers(),
                    maxInFlight,
                    firstSendAtNanos,
                    schedulingDeadlineNanos,
                    new PreparedHttpLoadDriver.ResponseRecorder() {
                        @Override
                        public void recordResponse(String side, int statusCode, long latencyNanos) {
                            counters.recordResponse(side, statusCode, latencyNanos, "");
                            if (secondaryCounters != null) {
                                secondaryCounters.recordResponse(side, statusCode, latencyNanos, "");
                            }
                        }

                        @Override
                        public void recordFailure(String side, Throwable failure) {
                            counters.recordFailure(side, failure);
                            if (secondaryCounters != null) {
                                secondaryCounters.recordFailure(side, failure);
                            }
                        }
                    });
        }

        private void submitLegacySteadyOrder(
                ExecutorService executor,
                Semaphore inFlight,
                CountDownLatch done,
                SteadyHttpCounters counters,
                SteadyHttpCounters secondaryCounters,
                String side,
                UUID orderId,
                UUID userId) throws InterruptedException {
            inFlight.acquire();
            executor.execute(() -> {
                long requestStartedAt = System.nanoTime();
                try {
                    boolean buy = "BUY".equals(side);
                    String body = buy
                            ? objectMapper.writeValueAsString(new BuyRequest(orderId, PRICE, AMOUNT, userId))
                            : objectMapper.writeValueAsString(new SellRequest(orderId, PRICE, AMOUNT, userId));
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(config.common().orderUrl() + (buy ? "/bid/buy" : "/bid/sell")))
                            .timeout(Duration.ofSeconds(10))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build();
                    HttpResponse<String> response = httpClient.send(
                            request, HttpResponse.BodyHandlers.ofString());
                    long latencyNanos = System.nanoTime() - requestStartedAt;
                    counters.recordResponse(side, response.statusCode(), latencyNanos, response.body());
                    if (secondaryCounters != null) {
                        secondaryCounters.recordResponse(
                                side, response.statusCode(), latencyNanos, response.body());
                    }
                } catch (Exception failure) {
                    counters.recordFailure(side, failure);
                    if (secondaryCounters != null) {
                        secondaryCounters.recordFailure(side, failure);
                    }
                } finally {
                    inFlight.release();
                    done.countDown();
                }
            });
        }

        private void monitor(
                List<SteadySample> samples,
                SteadyHttpCounters counters,
                AtomicBoolean running,
                long startedAtNanos,
                SteadyDatabaseBaseline baseline) {
            try (DatabaseHandles monitorDatabases = DatabaseHandles.open(config.common())) {
                long nextSampleAt = System.nanoTime();
                long lastProgressBucket = -1;
                while (running.get()) {
                    long waitNanos = nextSampleAt - System.nanoTime();
                    if (waitNanos > 0) {
                        TimeUnit.NANOSECONDS.sleep(waitNanos);
                    }
                    SteadySample sample = collectSample(
                            monitorDatabases, counters, startedAtNanos, baseline);
                    samples.add(sample);
                    long progressBucket = (long) sample.elapsedSeconds() / config.progressIntervalSeconds();
                    if (progressBucket != lastProgressBucket) {
                        lastProgressBucket = progressBucket;
                        System.out.printf(
                                "steady progress elapsed=%.1fs accepted=%d completed=%d "
                                        + "match=%d order=%d wallet=%d queueBacklog=%d failures=%d%n",
                                sample.elapsedSeconds(),
                                sample.httpAccepted(),
                                sample.completedTrades(),
                                sample.matchTrades(),
                                sample.orderTrades(),
                                sample.walletTrades(),
                                sample.queueBacklog(),
                                sample.httpFailures());
                    }
                    nextSampleAt += TimeUnit.SECONDS.toNanos(config.sampleIntervalSeconds());
                }
                samples.add(collectSample(monitorDatabases, counters, startedAtNanos, baseline));
            } catch (Exception e) {
                throw new IllegalStateException("steady-state monitor failed", e);
            }
        }

        private SteadySample collectSample(
                DatabaseHandles databases,
                SteadyHttpCounters counters,
                long startedAtNanos,
                SteadyDatabaseBaseline baseline) throws Exception {
            Config common = config.common();
            long matchTrades = runDelta(common.resetData()
                    ? queryLong(databases.match(), "SELECT count(*) FROM match_engine.trade_executions")
                    : queryLong(
                            databases.match(),
                            "SELECT count(*) FROM match_engine.trade_executions WHERE market_id = ?",
                            common.marketId()), baseline.matchTrades(), "match trades");
            long orderTrades = runDelta(countTradesByPrefix(
                    common,
                    databases.order(),
                    "SELECT count(*) FROM order_service.order_trade_applications WHERE left(trade_id, ?) = ?",
                    "SELECT count(*) FROM order_service.order_trade_applications",
                    common.marketId()), baseline.orderTrades(), "order trades");
            long walletTrades = runDelta(countTradesByPrefix(
                    common,
                    databases.wallet(),
                    "SELECT count(*) FROM wallet_service.trade_settlements WHERE left(trade_id, ?) = ?",
                    "SELECT count(*) FROM wallet_service.trade_settlements",
                    common.marketId()), baseline.walletTrades(), "wallet trades");
            QueueSnapshot queues = readQueues(config.common(), httpClient, objectMapper);
            return new SteadySample(
                    elapsedSince(startedAtNanos),
                    counters.accepted(),
                    counters.failures(),
                    matchTrades,
                    orderTrades,
                    walletTrades,
                    Math.min(matchTrades, Math.min(orderTrades, walletTrades)),
                    queues.backlog(),
                    queues.readFailures());
        }

        private SteadyWindow deriveSteadyWindow(List<SteadySample> samples) {
            return deriveWindow(
                    samples,
                    config.warmupSeconds(),
                    config.warmupSeconds() + config.durationSeconds(),
                    config.targetOrderTps());
        }

        private SteadyWindow deriveWindow(
                List<SteadySample> samples,
                double windowStartSeconds,
                double windowEndSeconds,
                int targetOrderTps) {
            List<SteadySample> windowSamples;
            synchronized (samples) {
                windowSamples = samples.stream()
                        .filter(sample -> sample.elapsedSeconds() >= windowStartSeconds)
                        .filter(sample -> sample.elapsedSeconds() <= windowEndSeconds)
                        .toList();
            }
            if (windowSamples.size() < 2) {
                return SteadyWindow.empty();
            }
            SteadySample first = windowSamples.get(0);
            SteadySample last = windowSamples.get(windowSamples.size() - 1);
            double seconds = last.elapsedSeconds() - first.elapsedSeconds();
            long accepted = last.httpAccepted() - first.httpAccepted();
            long completed = last.completedTrades() - first.completedTrades();
            double acceptedTps = accepted / Math.max(seconds, 0.001);
            double completedTps = completed / Math.max(seconds, 0.001);
            double completionToAcceptedRatio = completed / Math.max(accepted / 2.0, 0.001);
            BacklogWindow backlog = summarizeBacklog(windowSamples);
            long queueReadFailures = windowSamples.stream().mapToLong(SteadySample::queueReadFailures).sum();
            return new SteadyWindow(
                    first.elapsedSeconds(),
                    last.elapsedSeconds(),
                    seconds,
                    accepted,
                    completed,
                    acceptedTps,
                    completedTps,
                    acceptedTps / targetOrderTps,
                    completedTps / (targetOrderTps / 2.0),
                    completionToAcceptedRatio,
                    backlog.start(),
                    backlog.end(),
                    backlog.max(),
                    backlog.slopePerSecond(),
                    queueReadFailures,
                    windowSamples.size());
        }

        private SteadyExpectedOutcome resolveExpectedOutcome(
                Connection order,
                Config common,
                SteadyHttpCounters counters,
                long submissionBaselinePosition) throws Exception {
            if (counters.failures() == 0) {
                return SteadyExpectedOutcome.balanced(common.events());
            }

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            SteadyExpectedOutcome previous = null;
            int stableSamples = 0;
            while (System.nanoTime() < deadline) {
                SteadyExpectedOutcome current = queryDurableAcceptedOutcome(
                        order,
                        common.marketId(),
                        submissionBaselinePosition);
                stableSamples = current.equals(previous) ? stableSamples + 1 : 1;
                previous = current;
                if (stableSamples >= 10) {
                    System.out.printf(
                            "resolved durable HTTP outcomes after client failures: "
                                    + "buy=%d, sell=%d, pairableTrades=%d, unmatchedBuy=%d, unmatchedSell=%d%n",
                            current.buyOrders(),
                            current.sellOrders(),
                            current.pairableTrades(),
                            current.unmatchedBuyOrders(),
                            current.unmatchedSellOrders());
                    return current;
                }
                TimeUnit.MILLISECONDS.sleep(500);
            }
            if (previous == null) {
                throw new IllegalStateException("Unable to resolve durable HTTP outcomes");
            }
            return previous;
        }

        private SteadyExpectedOutcome queryDurableAcceptedOutcome(
                Connection order,
                String marketId,
                long submissionBaselinePosition) throws Exception {
            try (PreparedStatement statement = order.prepareStatement("""
                    SELECT count(*) FILTER (
                               WHERE payload_canonical::jsonb ->> 'side' = 'BUY') AS buy_orders,
                           count(*) FILTER (
                               WHERE payload_canonical::jsonb ->> 'side' = 'SELL') AS sell_orders
                    FROM order_service.order_event_store
                    WHERE event_type = 'OrderSubmissionRequestedV1'
                      AND global_position > ?
                      AND payload_canonical::jsonb ->> 'marketId' = ?
                    """)) {
                statement.setLong(1, submissionBaselinePosition);
                statement.setString(2, marketId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        return new SteadyExpectedOutcome(0, 0);
                    }
                    return new SteadyExpectedOutcome(
                            resultSet.getLong("buy_orders"),
                            resultSet.getLong("sell_orders"));
                }
            }
        }

        private SteadyCompletion waitForSteadyCompletion(
                DatabaseHandles databases,
                List<UUID> buyers,
                List<UUID> sellers,
                RoleBalances initialBuyerBalances,
                RoleBalances initialSellerBalances,
                long startedAtNanos,
                Config common,
                SteadyExpectedOutcome expectedOutcome,
                SteadyDatabaseBaseline baseline) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(common.waitTimeoutSeconds());
            long queueReadFailures = 0;
            int consecutiveDrainedSamples = 0;
            SteadyCompletion latest = SteadyCompletion.empty();

            while (System.nanoTime() < deadline) {
                long submitted = runDelta(queryLong(
                        databases.order(),
                        "SELECT count(*) FROM order_service.order_event_store WHERE event_type = ?",
                        "OrderSubmissionRequestedV1"), baseline.submitted(), "submitted orders");
                long reservationClaims = runDelta(queryLong(
                        databases.wallet(),
                        "SELECT count(*) FROM wallet_service.order_submission_idempotency"),
                        baseline.reservationClaims(), "reservation claims");
                long confirmed = runDelta(queryLong(
                        databases.order(),
                        "SELECT count(*) FROM order_service.order_event_store WHERE event_type = ?",
                        "OrderAssetReservationConfirmedV1"), baseline.confirmed(), "confirmed orders");
                long matchedOrders = runDelta(queryLong(
                        databases.order(),
                        """
                                SELECT count(*)
                                FROM order_service.order_matching_state
                                WHERE status = 'MATCHED'
                                  AND remaining_amount = 0
                                """), baseline.matchedOrders(), "matched orders");
                long matchTrades = runDelta(common.resetData()
                        ? queryLong(databases.match(), "SELECT count(*) FROM match_engine.trade_executions")
                        : queryLong(
                                databases.match(),
                                "SELECT count(*) FROM match_engine.trade_executions WHERE market_id = ?",
                                common.marketId()), baseline.matchTrades(), "match trades");
                long orderTrades = runDelta(countTradesByPrefix(
                        common,
                        databases.order(),
                        "SELECT count(*) FROM order_service.order_trade_applications WHERE left(trade_id, ?) = ?",
                        "SELECT count(*) FROM order_service.order_trade_applications",
                        common.marketId()), baseline.orderTrades(), "order trades");
                long walletTrades = runDelta(countTradesByPrefix(
                        common,
                        databases.wallet(),
                        "SELECT count(*) FROM wallet_service.trade_settlements WHERE left(trade_id, ?) = ?",
                        "SELECT count(*) FROM wallet_service.trade_settlements",
                        common.marketId()), baseline.walletTrades(), "wallet trades");
                QueueSnapshot queues = readQueues(common, httpClient, objectMapper);
                queueReadFailures += queues.readFailures();

                RoleBalances buyerBalances = RoleBalances.empty();
                RoleBalances sellerBalances = RoleBalances.empty();
                long buyBook = -1;
                long sellBook = -1;
                long activeReservations = -1;
                TradeIdDigestCheck tradeIds = TradeIdDigestCheck.empty();
                boolean countsReached = submitted == expectedOutcome.acceptedOrders()
                        && reservationClaims == expectedOutcome.acceptedOrders()
                        && confirmed == expectedOutcome.acceptedOrders()
                        && matchedOrders == expectedOutcome.pairableTrades() * 2L
                        && matchTrades == expectedOutcome.pairableTrades()
                        && orderTrades == expectedOutcome.pairableTrades()
                        && walletTrades == expectedOutcome.pairableTrades();
                if (countsReached) {
                    buyerBalances = readRoleBalances(databases.wallet(), buyers);
                    sellerBalances = readRoleBalances(databases.wallet(), sellers);
                    buyBook = redisInteger(common, "ZCARD", orderbookKey(common.marketId(), "buy"));
                    sellBook = redisInteger(common, "ZCARD", orderbookKey(common.marketId(), "sell"));
                    activeReservations = countRedisKeys(common, "order:reservation:*");
                }

                boolean queuesDrained = queues.backlog() == 0 && queues.readFailures() == 0;
                consecutiveDrainedSamples = queuesDrained ? consecutiveDrainedSamples + 1 : 0;
                boolean assetsSettled = countsReached && balancesConverged(
                        expectedOutcome,
                        initialBuyerBalances,
                        initialSellerBalances,
                        buyerBalances,
                        sellerBalances);
                if (countsReached
                        && assetsSettled
                        && buyBook == expectedOutcome.unmatchedBuyOrders()
                        && sellBook == expectedOutcome.unmatchedSellOrders()
                        && activeReservations == 0
                        && consecutiveDrainedSamples >= 3) {
                    tradeIds = digestTradeIds(
                            databases,
                            common,
                            baseline.matchTrades() + expectedOutcome.pairableTrades());
                }
                latest = new SteadyCompletion(
                        submitted,
                        reservationClaims,
                        confirmed,
                        matchedOrders,
                        matchTrades,
                        orderTrades,
                        walletTrades,
                        buyerBalances,
                        sellerBalances,
                        buyBook,
                        sellBook,
                        activeReservations,
                        tradeIds,
                        queues.backlog(),
                        queueReadFailures,
                        queues.depths(),
                        elapsedSince(startedAtNanos));
                if (tradeIds.equal()) {
                    return latest;
                }
                TimeUnit.MILLISECONDS.sleep(500);
            }
            return latest;
        }

        private static boolean balancesConverged(
                SteadyExpectedOutcome expected,
                RoleBalances initialBuyer,
                RoleBalances initialSeller,
                RoleBalances buyer,
                RoleBalances seller) {
            long tradedAmount = expected.pairableTrades() * AMOUNT;
            long tradedCurrency = expected.pairableTrades() * PRICE * (long) AMOUNT;
            long reservedBuyCurrency = expected.buyOrders() * PRICE * (long) AMOUNT;
            long reservedSellAmount = expected.sellOrders() * AMOUNT;
            return buyer.availableAmount() == initialBuyer.availableAmount() + tradedAmount
                    && buyer.availableCurrency()
                    == initialBuyer.availableCurrency() - reservedBuyCurrency
                    && buyer.lockedAmount() == 0
                    && buyer.lockedCurrency()
                    == expected.unmatchedBuyOrders() * PRICE * (long) AMOUNT
                    && seller.availableAmount()
                    == initialSeller.availableAmount() - reservedSellAmount
                    && seller.availableCurrency()
                    == initialSeller.availableCurrency() + tradedCurrency
                    && seller.lockedAmount() == expected.unmatchedSellOrders() * AMOUNT
                    && seller.lockedCurrency() == 0;
        }

        private TradeIdDigestCheck digestTradeIds(
                DatabaseHandles databases,
                Config common,
                long expectedTrades) throws Exception {
            TradeIdDigest match = digestTradeIds(
                    databases.match(),
                    "SELECT trade_id FROM match_engine.trade_executions WHERE market_id = ? ORDER BY trade_id",
                    common.marketId());
            String prefix = common.marketId() + "-";
            TradeIdDigest order = digestTradeIdsByPrefix(
                    databases.order(),
                    "SELECT trade_id FROM order_service.order_trade_applications "
                            + "WHERE left(trade_id, ?) = ? ORDER BY trade_id",
                    prefix);
            TradeIdDigest wallet = digestTradeIdsByPrefix(
                    databases.wallet(),
                    "SELECT trade_id FROM wallet_service.trade_settlements "
                            + "WHERE left(trade_id, ?) = ? ORDER BY trade_id",
                    prefix);
            boolean equal = match.count() == expectedTrades
                    && match.equals(order)
                    && match.equals(wallet);
            return new TradeIdDigestCheck(
                    equal,
                    match.count(),
                    order.count(),
                    wallet.count(),
                    match.fingerprint());
        }

        private TradeIdDigest digestTradeIds(
                Connection connection,
                String sql,
                String marketId) throws Exception {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, marketId);
                return digestRows(statement);
            }
        }

        private TradeIdDigest digestTradeIdsByPrefix(
                Connection connection,
                String sql,
                String prefix) throws Exception {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, prefix.length());
                statement.setString(2, prefix);
                return digestRows(statement);
            }
        }

        private TradeIdDigest digestRows(PreparedStatement statement) throws Exception {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            long count = 0;
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    digest.update(resultSet.getString(1).getBytes(StandardCharsets.UTF_8));
                    digest.update((byte) 0);
                    count++;
                }
            }
            return new TradeIdDigest(count, HexFormat.of().formatHex(digest.digest()));
        }

        private long queryLong(Connection connection, String sql, Object... values) throws Exception {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (int i = 0; i < values.length; i++) {
                    statement.setObject(i + 1, values[i]);
                }
                try (ResultSet resultSet = statement.executeQuery()) {
                    return resultSet.next() ? resultSet.getLong(1) : 0;
                }
            }
        }

        private SteadyDatabaseBaseline captureDatabaseBaseline(
                DatabaseHandles databases,
                Config common) throws Exception {
            long matchTrades = common.resetData()
                    ? queryLong(databases.match(), "SELECT count(*) FROM match_engine.trade_executions")
                    : queryLong(
                            databases.match(),
                            "SELECT count(*) FROM match_engine.trade_executions WHERE market_id = ?",
                            common.marketId());
            long orderTrades = countTradesByPrefix(
                    common,
                    databases.order(),
                    "SELECT count(*) FROM order_service.order_trade_applications WHERE left(trade_id, ?) = ?",
                    "SELECT count(*) FROM order_service.order_trade_applications",
                    common.marketId());
            long walletTrades = countTradesByPrefix(
                    common,
                    databases.wallet(),
                    "SELECT count(*) FROM wallet_service.trade_settlements WHERE left(trade_id, ?) = ?",
                    "SELECT count(*) FROM wallet_service.trade_settlements",
                    common.marketId());
            return new SteadyDatabaseBaseline(
                    queryLong(
                            databases.order(),
                            "SELECT count(*) FROM order_service.order_event_store WHERE event_type = ?",
                            "OrderSubmissionRequestedV1"),
                    queryLong(
                            databases.wallet(),
                            "SELECT count(*) FROM wallet_service.order_submission_idempotency"),
                    queryLong(
                            databases.order(),
                            "SELECT count(*) FROM order_service.order_event_store WHERE event_type = ?",
                            "OrderAssetReservationConfirmedV1"),
                    queryLong(
                            databases.order(),
                            """
                                    SELECT count(*)
                                    FROM order_service.order_matching_state
                                    WHERE status = 'MATCHED'
                                      AND remaining_amount = 0
                                    """),
                    matchTrades,
                    orderTrades,
                    walletTrades);
        }

        private List<String> steadyInvalidReasons(
                SteadyHttpCounters counters,
                SteadyWindow window,
                SteadyCompletion completion,
                RoleBalances initialBuyerBalances,
                RoleBalances initialSellerBalances,
                SteadyExpectedOutcome expectedOutcome) {
            Config common = config.common();
            List<String> reasons = new ArrayList<>();
            if (counters.accepted() != common.events() * 2L) {
                reasons.add("http_accepted_count_mismatch");
            }
            if (counters.tooManyRequests.get() != 0) {
                reasons.add("http_429");
            }
            if (counters.unavailable.get() != 0) {
                reasons.add("http_503");
            }
            if (counters.otherFailures.get() != 0) {
                reasons.add("http_other_failures");
            }
            if (counters.unscheduled.get() != 0) {
                reasons.add("traffic_scheduling_deadline_exceeded");
            }
            if (window.samples() < 2) {
                reasons.add("insufficient_steady_samples");
            }
            if (window.offeredLoadRatio() < config.minOfferedLoadRatio()) {
                reasons.add("steady_offered_load_below_minimum");
            }
            if (window.completionTargetRatio() < config.minCompletionRatio()) {
                reasons.add("steady_completion_rate_below_minimum");
            }
            if (exceedsMeaningfulBacklogGrowth(
                    window.backlogSlopePerSecond(),
                    window.startBacklog(),
                    window.endBacklog(),
                    window.observedSeconds(),
                    config.targetOrderTps(),
                    config.maxBacklogGrowthPerSecond())) {
                reasons.add("steady_queue_backlog_growing");
            }
            if (window.maxBacklog() > config.maxSteadyBacklog()) {
                reasons.add("steady_queue_backlog_above_limit");
            }
            if (window.queueReadFailures() != 0) {
                reasons.add("steady_queue_metrics_read_failures");
            }
            if (completion.submitted() != expectedOutcome.acceptedOrders()
                    || completion.reservationClaims() != expectedOutcome.acceptedOrders()
                    || completion.confirmed() != expectedOutcome.acceptedOrders()
                    || completion.matchedOrders() != expectedOutcome.pairableTrades() * 2L) {
                reasons.add("final_order_fact_count_mismatch");
            }
            if (completion.matchTrades() != expectedOutcome.pairableTrades()
                    || completion.orderTrades() != expectedOutcome.pairableTrades()
                    || completion.walletTrades() != expectedOutcome.pairableTrades()) {
                reasons.add("final_trade_fact_count_mismatch");
            }
            if (!completion.tradeIds().equal()) {
                reasons.add("three_service_trade_id_digest_mismatch");
            }
            if (!balancesConverged(
                    expectedOutcome,
                    initialBuyerBalances,
                    initialSellerBalances,
                    completion.buyerBalances(),
                    completion.sellerBalances())) {
                reasons.add("asset_settlement_mismatch");
            }
            if (completion.buyBook() != expectedOutcome.unmatchedBuyOrders()
                    || completion.sellBook() != expectedOutcome.unmatchedSellOrders()) {
                reasons.add("orderbook_count_mismatch");
            }
            if (completion.activeReservations() != 0) {
                reasons.add("match_reservations_not_drained");
            }
            if (completion.finalQueueBacklog() != 0) {
                reasons.add("final_queue_backlog");
            }
            if (completion.queueReadFailures() != 0) {
                reasons.add("final_queue_metrics_read_failures");
            }
            return reasons;
        }

        private void writeSamples(List<SteadySample> samples) throws IOException {
            if (config.sampleOutput().isBlank()) {
                return;
            }
            Path output = Path.of(config.sampleOutput());
            if (output.getParent() != null) {
                Files.createDirectories(output.getParent());
            }
            try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
                writer.write("elapsed_seconds,http_accepted,http_failures,match_trades,order_trades,"
                        + "wallet_trades,completed_trades,queue_backlog,queue_read_failures\n");
                synchronized (samples) {
                    for (SteadySample sample : samples) {
                        writer.write(String.format(
                                java.util.Locale.ROOT,
                                "%.4f,%d,%d,%d,%d,%d,%d,%d,%d%n",
                                sample.elapsedSeconds(),
                                sample.httpAccepted(),
                                sample.httpFailures(),
                                sample.matchTrades(),
                                sample.orderTrades(),
                                sample.walletTrades(),
                                sample.completedTrades(),
                                sample.queueBacklog(),
                                sample.queueReadFailures()));
                    }
                }
            }
        }

        private void printSteadyResult(
                SteadyHttpCounters counters,
                SteadyWindow window,
                SteadyCompletion completion,
                RoleBalances initialBuyerBalances,
                RoleBalances initialSellerBalances,
                double preparationSeconds,
                double sendElapsedSeconds,
                double fullConvergenceSeconds,
                SteadyExpectedOutcome expectedOutcome,
                List<String> invalidReasons) {
            Config common = config.common();
            System.out.println("{");
            System.out.println("  \"benchmarkSchemaVersion\": 2,");
            String benchmarkContract = "external-vegeta".equals(config.httpDriverMode())
                    ? ExternalHttpMatchedManifest.CONTRACT
                    : "http-matched-steady-state-chain";
            System.out.printf("  \"benchmarkContract\": \"%s\",%n", benchmarkContract);
            System.out.printf("  \"runId\": \"%s\",%n", json(common.runId()));
            System.out.printf("  \"marketId\": \"%s\",%n", json(common.marketId()));
            System.out.printf("  \"runtimeProfile\": \"%s\",%n", json(config.runtimeProfile()));
            System.out.printf("  \"arrivalPattern\": \"%s\",%n",
                    config.arrivalPattern().externalName());
            System.out.printf("  \"workloadSeed\": %d,%n", config.workloadSeed());
            System.out.printf("  \"warmupSeconds\": %d,%n", config.warmupSeconds());
            System.out.printf("  \"measurementSeconds\": %d,%n", config.durationSeconds());
            System.out.printf("  \"targetTotalOrderTps\": %d,%n", config.targetOrderTps());
            System.out.printf("  \"httpDriverMode\": \"%s\",%n", config.httpDriverMode());
            if ("external-vegeta".equals(config.httpDriverMode())) {
                System.out.println("  \"loadGeneratorMaxInFlight\": null,");
            } else {
                System.out.printf("  \"loadGeneratorMaxInFlight\": %d,%n", common.maxInFlight());
            }
            System.out.printf("  \"workloadPreparationSeconds\": %.4f,%n", preparationSeconds);
            System.out.printf("  \"expectedHttpOrders\": %d,%n", common.events() * 2L);
            System.out.printf("  \"expectedTrades\": %d,%n", common.events());
            System.out.printf("  \"durableBuyOrders\": %d,%n", expectedOutcome.buyOrders());
            System.out.printf("  \"durableSellOrders\": %d,%n", expectedOutcome.sellOrders());
            System.out.printf("  \"pairableAcceptedTrades\": %d,%n",
                    expectedOutcome.pairableTrades());
            System.out.printf("  \"expectedRemainingBuyOrders\": %d,%n",
                    expectedOutcome.unmatchedBuyOrders());
            System.out.printf("  \"expectedRemainingSellOrders\": %d,%n",
                    expectedOutcome.unmatchedSellOrders());
            System.out.printf("  \"usersPerSide\": %d,%n", common.usersPerSide());
            System.out.printf("  \"httpAccepted\": %d,%n", counters.accepted());
            System.out.printf("  \"sellHttpAccepted\": %d,%n", counters.sellAccepted.get());
            System.out.printf("  \"buyHttpAccepted\": %d,%n", counters.buyAccepted.get());
            System.out.printf("  \"http429\": %d,%n", counters.tooManyRequests.get());
            System.out.printf("  \"http503\": %d,%n", counters.unavailable.get());
            System.out.printf("  \"httpOtherFailures\": %d,%n", counters.otherFailures.get());
            System.out.printf("  \"unscheduledOrders\": %d,%n", counters.unscheduled.get());
            System.out.printf("  \"trafficSendSeconds\": %.4f,%n", sendElapsedSeconds);
            System.out.printf("  \"totalHttpAcceptedTps\": %.2f,%n",
                    counters.accepted() / Math.max(sendElapsedSeconds, 0.001));
            System.out.printf("  \"httpLatencyP50UpperBoundMs\": %d,%n",
                    counters.latency.percentileUpperBoundMillis(0.50));
            System.out.printf("  \"httpLatencyP95UpperBoundMs\": %d,%n",
                    counters.latency.percentileUpperBoundMillis(0.95));
            System.out.printf("  \"httpLatencyP99UpperBoundMs\": %d,%n",
                    counters.latency.percentileUpperBoundMillis(0.99));
            System.out.printf("  \"steadyWindowStartSeconds\": %.4f,%n", window.startSeconds());
            System.out.printf("  \"steadyWindowEndSeconds\": %.4f,%n", window.endSeconds());
            System.out.printf("  \"steadyWindowObservedSeconds\": %.4f,%n", window.observedSeconds());
            System.out.printf("  \"steadySampleCount\": %d,%n", window.samples());
            System.out.printf("  \"steadyAcceptedOrders\": %d,%n", window.acceptedOrders());
            System.out.printf("  \"steadyCompletedTrades\": %d,%n", window.completedTrades());
            System.out.printf("  \"steadyAcceptedOrderTps\": %.2f,%n", window.acceptedOrderTps());
            System.out.printf("  \"steadyCompletedTradeTps\": %.2f,%n", window.completedTradeTps());
            System.out.printf("  \"steadyOfferedLoadRatio\": %.4f,%n", window.offeredLoadRatio());
            System.out.printf("  \"steadyCompletionTargetRatio\": %.4f,%n", window.completionTargetRatio());
            System.out.printf("  \"steadyCompletionToAcceptedRatio\": %.4f,%n",
                    window.completionToAcceptedRatio());
            System.out.printf("  \"steadyBacklogStart\": %d,%n", window.startBacklog());
            System.out.printf("  \"steadyBacklogEnd\": %d,%n", window.endBacklog());
            System.out.printf("  \"steadyMaxBacklog\": %d,%n", window.maxBacklog());
            System.out.printf("  \"steadyBacklogSlopePerSecond\": %.4f,%n",
                    window.backlogSlopePerSecond());
            System.out.printf("  \"minOfferedLoadRatio\": %.4f,%n", config.minOfferedLoadRatio());
            System.out.printf("  \"minCompletionRatio\": %.4f,%n", config.minCompletionRatio());
            System.out.printf("  \"maxBacklogGrowthPerSecond\": %.4f,%n",
                    config.maxBacklogGrowthPerSecond());
            System.out.printf("  \"maxSteadyBacklog\": %d,%n", config.maxSteadyBacklog());
            System.out.printf("  \"finalOrderSubmissionRows\": %d,%n", completion.submitted());
            System.out.printf("  \"finalWalletReservationRows\": %d,%n", completion.reservationClaims());
            System.out.printf("  \"finalOrderConfirmationRows\": %d,%n", completion.confirmed());
            System.out.printf("  \"finalMatchedOrderRows\": %d,%n", completion.matchedOrders());
            System.out.printf("  \"finalMatchTradeRows\": %d,%n", completion.matchTrades());
            System.out.printf("  \"finalOrderTradeRows\": %d,%n", completion.orderTrades());
            System.out.printf("  \"finalWalletTradeRows\": %d,%n", completion.walletTrades());
            System.out.printf("  \"threeServiceTradeIdsEqual\": %s,%n", completion.tradeIds().equal());
            System.out.printf("  \"matchTradeIdCount\": %d,%n", completion.tradeIds().matchCount());
            System.out.printf("  \"orderTradeIdCount\": %d,%n", completion.tradeIds().orderCount());
            System.out.printf("  \"walletTradeIdCount\": %d,%n", completion.tradeIds().walletCount());
            System.out.printf("  \"tradeIdFingerprint\": \"%s\",%n",
                    completion.tradeIds().fingerprint());
            printBalances("initialBuyer", initialBuyerBalances);
            printBalances("initialSeller", initialSellerBalances);
            printBalances("finalBuyer", completion.buyerBalances());
            printBalances("finalSeller", completion.sellerBalances());
            System.out.printf("  \"remainingBuyOrders\": %d,%n", completion.buyBook());
            System.out.printf("  \"remainingSellOrders\": %d,%n", completion.sellBook());
            System.out.printf("  \"activeMatchReservations\": %d,%n", completion.activeReservations());
            System.out.printf("  \"finalQueueBacklog\": %d,%n", completion.finalQueueBacklog());
            System.out.printf("  \"queueMetricsReadFailures\": %d,%n",
                    window.queueReadFailures() + completion.queueReadFailures());
            printQueueDepths(completion.finalQueueDepths());
            System.out.printf("  \"fullConvergenceSeconds\": %.4f,%n", fullConvergenceSeconds);
            System.out.printf("  \"fullConvergenceTradeTps\": %.2f,%n",
                    expectedOutcome.pairableTrades()
                            / Math.max(fullConvergenceSeconds, 0.001));
            System.out.printf("  \"sampleOutput\": \"%s\",%n", json(config.sampleOutput()));
            System.out.printf("  \"validForSustainedCapacity\": %s,%n", invalidReasons.isEmpty());
            System.out.printf("  \"capacityInvalidReasons\": %s%n", jsonArray(invalidReasons));
            System.out.println("}");
        }

        private UUID deterministicSteadyOrderId(String runId, String side, int index) {
            return UUID.nameUUIDFromBytes(
                    (runId + ":steady:" + side + ":" + index).getBytes(StandardCharsets.UTF_8));
        }
    }

    private static final class ExternalSteadyStateRunner {

        private final String[] args;
        private final SteadyStateConfig config;
        private final SteadyStateRunner base;
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        private final Path manifestPath;
        private final Path targetsPath;
        private final Path monitorOutputPath;
        private final Path monitorReadyPath;
        private final Path monitorStopPath;
        private final Path vegetaResultsPath;

        private ExternalSteadyStateRunner(String[] args) {
            this.args = args.clone();
            this.config = SteadyStateConfig.from(args);
            if (!"external-vegeta".equals(config.httpDriverMode())) {
                throw new IllegalArgumentException(
                        "external lifecycle requires --http-driver-mode external-vegeta");
            }
            this.base = new SteadyStateRunner(config);
            this.manifestPath = requiredPath("--manifest");
            this.targetsPath = pathArg("--targets", "");
            this.monitorOutputPath = pathArg("--monitor-output", "");
            this.monitorReadyPath = pathArg("--monitor-ready", "");
            this.monitorStopPath = pathArg("--monitor-stop", "");
            this.vegetaResultsPath = pathArg("--vegeta-results", "");
        }

        private void prepare() throws Exception {
            requirePath(targetsPath, "--targets");
            Config common = config.common();
            validateMarket(common);
            if (common.resetData()) {
                reset(common, httpClient, objectMapper);
            }

            System.out.printf(
                    "preparing external open-loop workload: orders=%d, target=%d/s%n",
                    common.events() * 2L,
                    config.targetOrderTps());
            List<UUID> buyers = registerUsers(common, httpClient, objectMapper, common.usersPerSide());
            List<UUID> sellers = registerUsers(common, httpClient, objectMapper, common.usersPerSide());
            List<UUID> allUsers = new ArrayList<>(buyers.size() + sellers.size());
            allUsers.addAll(buyers);
            allUsers.addAll(sellers);

            try (DatabaseHandles databases = DatabaseHandles.open(common)) {
                base.fundSteadyStateUsers(databases.wallet(), allUsers, common.events());
                RoleBalances initialBuyerBalances = readRoleBalances(databases.wallet(), buyers);
                RoleBalances initialSellerBalances = readRoleBalances(databases.wallet(), sellers);
                long submissionBaselinePosition = base.queryLong(
                        databases.order(),
                        "SELECT COALESCE(max(global_position), 0) FROM order_service.order_event_store");
                SteadyDatabaseBaseline databaseBaseline = base.captureDatabaseBaseline(databases, common);
                List<PreparedHttpLoadDriver.PreparedOrder> orders = base.prepareSteadyOrders(
                        buyers,
                        sellers,
                        0,
                        common.events(),
                        config.arrivalPattern(),
                        config.workloadSeed());
                writeVegetaTargets(orders);
                ExternalHttpMatchedManifest manifest = new ExternalHttpMatchedManifest(
                        ExternalHttpMatchedManifest.SCHEMA_VERSION,
                        ExternalHttpMatchedManifest.CONTRACT,
                        common.runId(),
                        common.marketId(),
                        config.targetOrderTps(),
                        config.warmupSeconds(),
                        config.durationSeconds(),
                        config.sampleIntervalSeconds(),
                        config.workloadSeed(),
                        config.arrivalPattern().externalName(),
                        common.usersPerSide(),
                        common.events() * 2L,
                        common.events(),
                        Instant.now().toEpochMilli(),
                        sha256(targetsPath),
                        buyers,
                        sellers,
                        balanceSnapshot(initialBuyerBalances),
                        balanceSnapshot(initialSellerBalances),
                        baselineSnapshot(databaseBaseline),
                        submissionBaselinePosition);
                createParent(manifestPath);
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(manifestPath.toFile(), manifest);
                System.out.printf(
                        "external workload prepared: manifest=%s, targets=%s, sha256=%s%n",
                        manifestPath,
                        targetsPath,
                        manifest.targetsSha256());
            }
        }

        private void monitor() throws Exception {
            requirePath(monitorOutputPath, "--monitor-output");
            requirePath(monitorReadyPath, "--monitor-ready");
            requirePath(monitorStopPath, "--monitor-stop");
            ExternalHttpMatchedManifest manifest = readManifest();
            validateManifest(manifest);
            SteadyDatabaseBaseline baseline = baseline(manifest.databaseBaseline());
            createParent(monitorOutputPath);
            createParent(monitorReadyPath);
            Files.deleteIfExists(monitorReadyPath);
            Files.deleteIfExists(monitorStopPath);

            try (DatabaseHandles databases = DatabaseHandles.open(config.common());
                 BufferedWriter writer = Files.newBufferedWriter(monitorOutputPath, StandardCharsets.UTF_8)) {
                writer.write("epoch_millis,match_trades,order_trades,wallet_trades,completed_trades,"
                        + "queue_backlog,queue_read_failures\n");
                writeMonitorSample(writer, databases, baseline);
                Files.writeString(monitorReadyPath, "ready\n", StandardCharsets.UTF_8);
                System.out.printf("external monitor ready: %s%n", monitorReadyPath);
                long nextSampleAt = System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(config.sampleIntervalSeconds());
                while (!Files.exists(monitorStopPath)) {
                    long remainingNanos = nextSampleAt - System.nanoTime();
                    if (remainingNanos > 0) {
                        TimeUnit.NANOSECONDS.sleep(Math.min(
                                remainingNanos,
                                TimeUnit.MILLISECONDS.toNanos(50)));
                        continue;
                    }
                    writeMonitorSample(writer, databases, baseline);
                    nextSampleAt += TimeUnit.SECONDS.toNanos(config.sampleIntervalSeconds());
                }
                writeMonitorSample(writer, databases, baseline);
            }
        }

        private void verify() throws Exception {
            requirePath(vegetaResultsPath, "--vegeta-results");
            requirePath(targetsPath, "--targets");
            requirePath(monitorOutputPath, "--monitor-output");
            ExternalHttpMatchedManifest manifest = readManifest();
            validateManifest(manifest);
            if (!Files.isRegularFile(targetsPath)
                    || !manifest.targetsSha256().equals(sha256(targetsPath))) {
                throw new IllegalStateException("prepared Vegeta target checksum does not match manifest");
            }

            ExternalHttpResults externalResults = readVegetaResults(manifest);
            SteadyHttpCounters counters = externalResults.counters();
            List<SteadySample> samples = mergeMonitorSamples(externalResults);
            SteadyWindow steadyWindow = base.deriveSteadyWindow(samples);
            RoleBalances initialBuyerBalances = roleBalances(manifest.initialBuyerBalances());
            RoleBalances initialSellerBalances = roleBalances(manifest.initialSellerBalances());
            SteadyDatabaseBaseline databaseBaseline = baseline(manifest.databaseBaseline());

            try (DatabaseHandles databases = DatabaseHandles.open(config.common())) {
                SteadyExpectedOutcome expectedOutcome = base.resolveExpectedOutcome(
                        databases.order(),
                        config.common(),
                        counters,
                        manifest.submissionBaselinePosition());
                long convergenceStartedAt = System.nanoTime();
                SteadyCompletion completion = base.waitForSteadyCompletion(
                        databases,
                        manifest.buyers(),
                        manifest.sellers(),
                        initialBuyerBalances,
                        initialSellerBalances,
                        convergenceStartedAt,
                        config.common(),
                        expectedOutcome,
                        databaseBaseline);
                double fullConvergenceSeconds = Math.max(
                        0.001,
                        (System.currentTimeMillis() - externalResults.firstRequestEpochMillis()) / 1_000.0);
                base.writeSamples(samples);
                List<String> invalidReasons = base.steadyInvalidReasons(
                        counters,
                        steadyWindow,
                        completion,
                        initialBuyerBalances,
                        initialSellerBalances,
                        expectedOutcome);
                base.printSteadyResult(
                        counters,
                        steadyWindow,
                        completion,
                        initialBuyerBalances,
                        initialSellerBalances,
                        0,
                        externalResults.responseCompletionSeconds(),
                        fullConvergenceSeconds,
                        expectedOutcome,
                        invalidReasons);
                if (!invalidReasons.isEmpty()) {
                    throw new IllegalStateException(
                            "external HTTP matched steady-state benchmark invalid: " + invalidReasons);
                }
            }
        }

        private void writeVegetaTargets(List<PreparedHttpLoadDriver.PreparedOrder> orders)
                throws IOException {
            createParent(targetsPath);
            try (BufferedWriter writer = Files.newBufferedWriter(targetsPath, StandardCharsets.UTF_8)) {
                for (PreparedHttpLoadDriver.PreparedOrder order : orders) {
                    Map<String, Object> target = new LinkedHashMap<>();
                    target.put("method", "POST");
                    target.put("url", order.uri().toString());
                    target.put("body", Base64.getEncoder().encodeToString(order.body()));
                    target.put("header", Map.of("Content-Type", List.of("application/json")));
                    writer.write(objectMapper.writeValueAsString(target));
                    writer.newLine();
                }
            }
        }

        private void writeMonitorSample(
                BufferedWriter writer,
                DatabaseHandles databases,
                SteadyDatabaseBaseline baseline) throws Exception {
            SteadySample sample = base.collectSample(
                    databases,
                    new SteadyHttpCounters(),
                    System.nanoTime(),
                    baseline);
            writer.write(String.format(
                    java.util.Locale.ROOT,
                    "%d,%d,%d,%d,%d,%d,%d%n",
                    System.currentTimeMillis(),
                    sample.matchTrades(),
                    sample.orderTrades(),
                    sample.walletTrades(),
                    sample.completedTrades(),
                    sample.queueBacklog(),
                    sample.queueReadFailures()));
            writer.flush();
        }

        private ExternalHttpResults readVegetaResults(ExternalHttpMatchedManifest manifest)
                throws IOException {
            SteadyHttpCounters counters = new SteadyHttpCounters();
            List<ExternalHttpResult> results = new ArrayList<>();
            try (var reader = Files.newBufferedReader(vegetaResultsPath, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    JsonNode result = objectMapper.readTree(line);
                    String url = result.path("url").asText();
                    String side;
                    if (url.endsWith("/bid/buy")) {
                        side = "BUY";
                    } else if (url.endsWith("/bid/sell")) {
                        side = "SELL";
                    } else {
                        throw new IOException("unexpected Vegeta target URL: " + url);
                    }
                    int status = result.path("code").asInt();
                    long latencyNanos = result.path("latency").asLong();
                    long requestEpochNanos = epochNanos(result.path("timestamp").asText());
                    counters.recordResponse(
                            side,
                            status,
                            latencyNanos,
                            result.path("error").asText());
                    results.add(new ExternalHttpResult(
                            requestEpochNanos,
                            Math.addExact(requestEpochNanos, Math.max(0, latencyNanos)),
                            status == 200 || status == 202));
                }
            }
            results.sort((left, right) -> Long.compare(left.requestEpochNanos(), right.requestEpochNanos()));
            long missing = manifest.expectedHttpOrders() - results.size();
            if (missing > 0) {
                counters.recordUnscheduled(Math.toIntExact(missing));
            } else if (missing < 0) {
                for (long extra = 0; extra < -missing; extra++) {
                    counters.recordResponse("UNKNOWN", 0, 0, "extra Vegeta result");
                }
            }
            if (results.isEmpty()) {
                throw new IllegalStateException("Vegeta result file contains no requests");
            }
            long first = results.get(0).requestEpochNanos();
            long lastCompletion = results.stream()
                    .mapToLong(ExternalHttpResult::completionEpochNanos)
                    .max()
                    .orElse(first);
            return new ExternalHttpResults(
                    counters,
                    List.copyOf(results),
                    first / 1_000_000L,
                    Math.max(0.001, (lastCompletion - first) / 1_000_000_000.0));
        }

        private List<SteadySample> mergeMonitorSamples(ExternalHttpResults externalResults)
                throws IOException {
            requirePath(monitorOutputPath, "--monitor-output");
            List<ExternalHttpResult> resultsByCompletion = new ArrayList<>(externalResults.results());
            resultsByCompletion.sort((left, right) ->
                    Long.compare(left.completionEpochNanos(), right.completionEpochNanos()));
            List<SteadySample> samples = new ArrayList<>();
            int completedResponses = 0;
            long accepted = 0;
            long failures = 0;
            List<String> lines = Files.readAllLines(monitorOutputPath, StandardCharsets.UTF_8);
            for (int lineNumber = 1; lineNumber < lines.size(); lineNumber++) {
                String line = lines.get(lineNumber);
                if (line.isBlank()) {
                    continue;
                }
                String[] values = line.split(",", -1);
                if (values.length != 7) {
                    throw new IOException("invalid external monitor CSV row: " + line);
                }
                long sampleEpochMillis = Long.parseLong(values[0]);
                long sampleEpochNanos = Math.multiplyExact(sampleEpochMillis, 1_000_000L);
                while (completedResponses < resultsByCompletion.size()
                        && resultsByCompletion.get(completedResponses).completionEpochNanos()
                        <= sampleEpochNanos) {
                    if (resultsByCompletion.get(completedResponses).accepted()) {
                        accepted++;
                    } else {
                        failures++;
                    }
                    completedResponses++;
                }
                samples.add(new SteadySample(
                        (sampleEpochNanos - externalResults.results().get(0).requestEpochNanos())
                                / 1_000_000_000.0,
                        accepted,
                        failures,
                        Long.parseLong(values[1]),
                        Long.parseLong(values[2]),
                        Long.parseLong(values[3]),
                        Long.parseLong(values[4]),
                        Long.parseLong(values[5]),
                        Long.parseLong(values[6])));
            }
            return List.copyOf(samples);
        }

        private ExternalHttpMatchedManifest readManifest() throws IOException {
            return objectMapper.readValue(manifestPath.toFile(), ExternalHttpMatchedManifest.class);
        }

        private void validateManifest(ExternalHttpMatchedManifest manifest) {
            if (manifest.manifestSchemaVersion() != ExternalHttpMatchedManifest.SCHEMA_VERSION
                    || !ExternalHttpMatchedManifest.CONTRACT.equals(manifest.benchmarkContract())
                    || !config.common().runId().equals(manifest.runId())
                    || !config.common().marketId().equals(manifest.marketId())
                    || config.targetOrderTps() != manifest.targetTotalOrderTps()
                    || config.warmupSeconds() != manifest.warmupSeconds()
                    || config.durationSeconds() != manifest.measurementSeconds()
                    || config.sampleIntervalSeconds() != manifest.sampleIntervalSeconds()
                    || config.workloadSeed() != manifest.workloadSeed()
                    || !config.arrivalPattern().externalName().equals(manifest.arrivalPattern())
                    || config.common().usersPerSide() != manifest.usersPerSide()
                    || config.common().events() * 2L != manifest.expectedHttpOrders()
                    || config.common().events() != manifest.expectedTrades()
                    || manifest.buyers().size() != manifest.usersPerSide()
                    || manifest.sellers().size() != manifest.usersPerSide()) {
                throw new IllegalStateException("external lifecycle arguments do not match prepared manifest");
            }
        }

        private void validateMarket(Config common) {
            if (!HTTP_MARKET_ID.equals(common.marketId())) {
                throw new IllegalArgumentException(
                        "Order HTTP currently assigns market " + HTTP_MARKET_ID + "; --market-id must match");
            }
        }

        private Path requiredPath(String name) {
            Path path = pathArg(name, "");
            requirePath(path, name);
            return path;
        }

        private Path pathArg(String name, String defaultValue) {
            String value = SteadyStateConfig.stringArg(args, name, defaultValue);
            return value.isBlank() ? null : Path.of(value);
        }

        private static void requirePath(Path path, String name) {
            if (path == null) {
                throw new IllegalArgumentException(name + " is required");
            }
        }

        private static void createParent(Path path) throws IOException {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
        }

        private static long epochNanos(String timestamp) {
            Instant instant = OffsetDateTime.parse(timestamp).toInstant();
            return Math.addExact(
                    Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L),
                    instant.getNano());
        }

        private static String sha256(Path path) throws IOException {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                try (var input = Files.newInputStream(path)) {
                    byte[] buffer = new byte[64 * 1_024];
                    int read;
                    while ((read = input.read(buffer)) >= 0) {
                        digest.update(buffer, 0, read);
                    }
                }
                return HexFormat.of().formatHex(digest.digest());
            } catch (NoSuchAlgorithmException impossible) {
                throw new IllegalStateException("SHA-256 unavailable", impossible);
            }
        }

        private static ExternalHttpMatchedManifest.BalanceSnapshot balanceSnapshot(RoleBalances source) {
            return new ExternalHttpMatchedManifest.BalanceSnapshot(
                    source.availableAmount(),
                    source.lockedAmount(),
                    source.availableCurrency(),
                    source.lockedCurrency());
        }

        private static RoleBalances roleBalances(ExternalHttpMatchedManifest.BalanceSnapshot source) {
            return new RoleBalances(
                    source.availableAmount(),
                    source.lockedAmount(),
                    source.availableCurrency(),
                    source.lockedCurrency());
        }

        private static ExternalHttpMatchedManifest.DatabaseBaselineSnapshot baselineSnapshot(
                SteadyDatabaseBaseline source) {
            return new ExternalHttpMatchedManifest.DatabaseBaselineSnapshot(
                    source.submitted(),
                    source.reservationClaims(),
                    source.confirmed(),
                    source.matchedOrders(),
                    source.matchTrades(),
                    source.orderTrades(),
                    source.walletTrades());
        }

        private static SteadyDatabaseBaseline baseline(
                ExternalHttpMatchedManifest.DatabaseBaselineSnapshot source) {
            return new SteadyDatabaseBaseline(
                    source.submitted(),
                    source.reservationClaims(),
                    source.confirmed(),
                    source.matchedOrders(),
                    source.matchTrades(),
                    source.orderTrades(),
                    source.walletTrades());
        }

        private record ExternalHttpResult(
                long requestEpochNanos,
                long completionEpochNanos,
                boolean accepted) {
        }

        private record ExternalHttpResults(
                SteadyHttpCounters counters,
                List<ExternalHttpResult> results,
                long firstRequestEpochMillis,
                double responseCompletionSeconds) {
        }
    }

    private static final class StaircaseRunner {

        private final StaircaseConfig config;
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        private StaircaseRunner(StaircaseConfig config) {
            this.config = config;
        }

        private void run() throws Exception {
            Config common = config.common();
            if (!HTTP_MARKET_ID.equals(common.marketId())) {
                throw new IllegalArgumentException(
                        "Order HTTP currently assigns market " + HTTP_MARKET_ID + "; --market-id must match");
            }
            if (common.resetData()) {
                reset(common, httpClient, objectMapper);
            }

            System.out.printf(
                    "registering %d buyers and %d sellers for staircase HTTP traffic%n",
                    common.usersPerSide(), common.usersPerSide());
            List<UUID> buyers = registerUsers(common, httpClient, objectMapper, common.usersPerSide());
            List<UUID> sellers = registerUsers(common, httpClient, objectMapper, common.usersPerSide());
            List<UUID> allUsers = new ArrayList<>(buyers.size() + sellers.size());
            allUsers.addAll(buyers);
            allUsers.addAll(sellers);

            SteadyStateRunner base = new SteadyStateRunner(config.asSteadyConfig());
            try (DatabaseHandles databases = DatabaseHandles.open(common)) {
                base.fundSteadyStateUsers(databases.wallet(), allUsers, common.events());
                RoleBalances initialBuyerBalances = readRoleBalances(databases.wallet(), buyers);
                RoleBalances initialSellerBalances = readRoleBalances(databases.wallet(), sellers);
                long submissionBaselinePosition = base.queryLong(
                        databases.order(),
                        "SELECT COALESCE(max(global_position), 0) FROM order_service.order_event_store");
                SteadyDatabaseBaseline databaseBaseline = base.captureDatabaseBaseline(databases, common);

                SteadyHttpCounters globalCounters = new SteadyHttpCounters();
                List<SteadySample> samples = Collections.synchronizedList(new ArrayList<>());
                List<StaircaseStageResult> stages = new ArrayList<>();
                AtomicBoolean monitorRunning = new AtomicBoolean(true);
                long startedAtNanos = System.nanoTime();
                ExecutorService monitorExecutor = Executors.newSingleThreadExecutor();
                Future<?> monitor = monitorExecutor.submit(() ->
                        base.monitor(
                                samples,
                                globalCounters,
                                monitorRunning,
                                startedAtNanos,
                                databaseBaseline));
                ExecutorService requestExecutor = Executors.newFixedThreadPool(common.workers());
                Semaphore inFlight = new Semaphore(common.maxInFlight());
                int sentTrades = 0;
                double trafficEndSeconds;

                try {
                    for (int targetOrderTps : config.targets()) {
                        double stageStartSeconds = elapsedSince(startedAtNanos);
                        System.out.printf(
                                "staircase stage target=%d total orders/s, warmup=%ds, measurement=%ds%n",
                                targetOrderTps,
                                config.stageWarmupSeconds(),
                                config.stageDurationSeconds());
                        StaircasePhaseSendResult warmup = sendPhase(
                                base,
                                requestExecutor,
                                inFlight,
                                buyers,
                                sellers,
                                globalCounters,
                                null,
                                sentTrades,
                                targetOrderTps,
                                config.stageWarmupSeconds());
                        sentTrades = warmup.nextTradeIndex();

                        SteadyHttpCounters measuredCounters = new SteadyHttpCounters();
                        double measurementStartSeconds = elapsedSince(startedAtNanos);
                        StaircasePhaseSendResult measurement = sendPhase(
                                base,
                                requestExecutor,
                                inFlight,
                                buyers,
                                sellers,
                                globalCounters,
                                measuredCounters,
                                sentTrades,
                                targetOrderTps,
                                config.stageDurationSeconds());
                        sentTrades = measurement.nextTradeIndex();
                        double measurementScheduledEndSeconds =
                                measurementStartSeconds + measurement.schedulingSeconds();
                        double measurementResponseEndSeconds =
                                measurementStartSeconds + measurement.responseCompletionSeconds();
                        awaitMonitorSample(samples, measurementScheduledEndSeconds);

                        SteadyWindow window = base.deriveWindow(
                                samples,
                                measurementStartSeconds,
                                measurementScheduledEndSeconds,
                                targetOrderTps);
                        int expectedMeasuredOrders =
                                Math.multiplyExact(targetOrderTps, config.stageDurationSeconds());
                        double maxBacklogGrowth = config.maxBacklogGrowthPerSecond(targetOrderTps);
                        long maxBacklog = config.maxSteadyBacklog(targetOrderTps);
                        List<String> reasons = stageInvalidReasons(
                                measuredCounters,
                                window,
                                expectedMeasuredOrders,
                                measurement.schedulingSeconds(),
                                targetOrderTps,
                                maxBacklogGrowth,
                                maxBacklog);
                        StaircaseStageResult stage = new StaircaseStageResult(
                                targetOrderTps,
                                stageStartSeconds,
                                measurementStartSeconds,
                                measurementScheduledEndSeconds,
                                measurementResponseEndSeconds,
                                measurement.schedulingSeconds(),
                                measurement.responseCompletionSeconds(),
                                measurement.responseDrainTailSeconds(),
                                expectedMeasuredOrders,
                                measuredCounters.accepted(),
                                measuredCounters.tooManyRequests.get(),
                                measuredCounters.unavailable.get(),
                                measuredCounters.otherFailures.get(),
                                measuredCounters.latency.percentileUpperBoundMillis(0.50),
                                measuredCounters.latency.percentileUpperBoundMillis(0.95),
                                measuredCounters.latency.percentileUpperBoundMillis(0.99),
                                window,
                                maxBacklogGrowth,
                                maxBacklog,
                                reasons);
                        stages.add(stage);
                        System.out.printf(
                                "staircase stage result target=%d offeredTps=%.2f responseTps=%.2f "
                                        + "responseTail=%.3fs completedTradeTps=%.2f backlogMax=%d "
                                        + "backlogSlope=%.2f passed=%s reasons=%s%n",
                                targetOrderTps,
                                stage.offeredOrderTps(),
                                stage.acceptedResponseTps(),
                                stage.responseDrainTailSeconds(),
                                window.completedTradeTps(),
                                window.maxBacklog(),
                                window.backlogSlopePerSecond(),
                                stage.passed(),
                                reasons);
                        if (!stage.passed() && config.stopOnFailedStage()) {
                            break;
                        }
                    }
                    trafficEndSeconds = elapsedSince(startedAtNanos);
                } finally {
                    requestExecutor.shutdown();
                    requestExecutor.awaitTermination(30, TimeUnit.SECONDS);
                    monitorRunning.set(false);
                    monitor.get();
                    monitorExecutor.shutdown();
                    monitorExecutor.awaitTermination(10, TimeUnit.SECONDS);
                }

                Config actualCommon = withEvents(common, sentTrades);
                SteadyExpectedOutcome expectedOutcome = base.resolveExpectedOutcome(
                        databases.order(),
                        actualCommon,
                        globalCounters,
                        submissionBaselinePosition);
                SteadyCompletion completion = base.waitForSteadyCompletion(
                        databases,
                        buyers,
                        sellers,
                        initialBuyerBalances,
                        initialSellerBalances,
                        startedAtNanos,
                        actualCommon,
                        expectedOutcome,
                        databaseBaseline);
                double fullConvergenceSeconds = elapsedSince(startedAtNanos);
                base.writeSamples(samples);
                writeStageSummary(stages);

                List<String> invalidReasons = finalInvalidReasons(
                        globalCounters,
                        completion,
                        initialBuyerBalances,
                        initialSellerBalances,
                        actualCommon,
                        expectedOutcome,
                        stages);
                printResult(
                        stages,
                        globalCounters,
                        completion,
                        initialBuyerBalances,
                        initialSellerBalances,
                        sentTrades,
                        expectedOutcome,
                        trafficEndSeconds,
                        fullConvergenceSeconds,
                        invalidReasons);
                if (!invalidReasons.isEmpty()) {
                    throw new IllegalStateException(
                            "HTTP matched staircase benchmark invalid: " + invalidReasons);
                }
            }
        }

        private StaircasePhaseSendResult sendPhase(
                SteadyStateRunner base,
                ExecutorService executor,
                Semaphore inFlight,
                List<UUID> buyers,
                List<UUID> sellers,
                SteadyHttpCounters globalCounters,
                SteadyHttpCounters phaseCounters,
                int startingTradeIndex,
                int targetOrderTps,
                int seconds) throws Exception {
            if (seconds == 0) {
                return new StaircasePhaseSendResult(startingTradeIndex, 0, 0, 0);
            }
            int totalOrders = Math.multiplyExact(targetOrderTps, seconds);
            int trades = totalOrders / 2;
            CountDownLatch done = new CountDownLatch(totalOrders);
            long phaseStartedAtNanos = System.nanoTime();
            AtomicLong nextSendAtNanos = new AtomicLong(phaseStartedAtNanos);
            long orderIntervalNanos = TimeUnit.SECONDS.toNanos(1) / targetOrderTps;
            long lastScheduledAtNanos = phaseStartedAtNanos;
            List<BalancedOrderSchedule.ScheduledOrder> schedule = BalancedOrderSchedule.create(
                    startingTradeIndex,
                    trades,
                    config.arrivalPattern(),
                    config.workloadSeed());
            for (BalancedOrderSchedule.ScheduledOrder scheduledOrder : schedule) {
                int index = scheduledOrder.tradeIndex();
                String side = scheduledOrder.side();
                List<UUID> users = "BUY".equals(side) ? buyers : sellers;
                int userIndex = scheduledOrder.userSequence();
                throttle(nextSendAtNanos, orderIntervalNanos);
                base.submitLegacySteadyOrder(
                        executor,
                        inFlight,
                        done,
                        globalCounters,
                        phaseCounters,
                        side,
                        base.deterministicSteadyOrderId(config.common().runId(), side, index),
                        users.get(userIndex % users.size()));
                lastScheduledAtNanos = System.nanoTime();
            }
            done.await();
            long responsesCompletedAtNanos = System.nanoTime();
            double schedulingSeconds =
                    (lastScheduledAtNanos - phaseStartedAtNanos + orderIntervalNanos)
                            / 1_000_000_000.0;
            double responseCompletionSeconds =
                    (responsesCompletedAtNanos - phaseStartedAtNanos) / 1_000_000_000.0;
            double responseDrainTailSeconds =
                    Math.max(0, (responsesCompletedAtNanos - lastScheduledAtNanos) / 1_000_000_000.0);
            return new StaircasePhaseSendResult(
                    Math.addExact(startingTradeIndex, trades),
                    schedulingSeconds,
                    responseCompletionSeconds,
                    responseDrainTailSeconds);
        }

        private void awaitMonitorSample(
                List<SteadySample> samples,
                double elapsedSeconds) throws InterruptedException {
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(config.sampleIntervalSeconds() + 2L);
            while (System.nanoTime() < deadline) {
                synchronized (samples) {
                    if (!samples.isEmpty()
                            && samples.get(samples.size() - 1).elapsedSeconds() >= elapsedSeconds) {
                        return;
                    }
                }
                TimeUnit.MILLISECONDS.sleep(50);
            }
        }

        private List<String> stageInvalidReasons(
                SteadyHttpCounters counters,
                SteadyWindow window,
                int expectedOrders,
                double schedulingSeconds,
                int targetOrderTps,
                double maxBacklogGrowth,
                long maxBacklog) {
            List<String> reasons = new ArrayList<>();
            if (counters.accepted() != expectedOrders) {
                reasons.add("http_accepted_count_mismatch");
            }
            if (counters.tooManyRequests.get() != 0) {
                reasons.add("http_429");
            }
            if (counters.unavailable.get() != 0) {
                reasons.add("http_503");
            }
            if (counters.otherFailures.get() != 0) {
                reasons.add("http_other_failures");
            }
            if (window.samples() < 2) {
                reasons.add("insufficient_steady_samples");
            }
            double offeredOrderTps = expectedOrders / Math.max(schedulingSeconds, 0.001);
            if (offeredOrderTps / targetOrderTps < config.minOfferedLoadRatio()) {
                reasons.add("stage_offered_load_below_minimum");
            }
            if (window.completionTargetRatio() < config.minCompletionRatio()) {
                reasons.add("stage_completion_rate_below_minimum");
            }
            if (exceedsMeaningfulBacklogGrowth(
                    window.backlogSlopePerSecond(),
                    window.startBacklog(),
                    window.endBacklog(),
                    window.observedSeconds(),
                    targetOrderTps,
                    maxBacklogGrowth)) {
                reasons.add("stage_queue_backlog_growing");
            }
            if (window.maxBacklog() > maxBacklog) {
                reasons.add("stage_queue_backlog_above_limit");
            }
            if (window.queueReadFailures() != 0) {
                reasons.add("stage_queue_metrics_read_failures");
            }
            return reasons;
        }

        private List<String> finalInvalidReasons(
                SteadyHttpCounters counters,
                SteadyCompletion completion,
                RoleBalances initialBuyerBalances,
                RoleBalances initialSellerBalances,
                Config actualCommon,
                SteadyExpectedOutcome expectedOutcome,
                List<StaircaseStageResult> stages) {
            List<String> reasons = new ArrayList<>();
            if (stages.stream().noneMatch(StaircaseStageResult::passed)) {
                reasons.add("no_sustainable_stage");
            }
            if (counters.accepted() != actualCommon.events() * 2L) {
                reasons.add("http_accepted_count_mismatch");
            }
            if (counters.failures() != 0) {
                reasons.add("http_failures");
            }
            if (completion.submitted() != expectedOutcome.acceptedOrders()
                    || completion.reservationClaims() != expectedOutcome.acceptedOrders()
                    || completion.confirmed() != expectedOutcome.acceptedOrders()
                    || completion.matchedOrders() != expectedOutcome.pairableTrades() * 2L) {
                reasons.add("final_order_fact_count_mismatch");
            }
            if (completion.matchTrades() != expectedOutcome.pairableTrades()
                    || completion.orderTrades() != expectedOutcome.pairableTrades()
                    || completion.walletTrades() != expectedOutcome.pairableTrades()) {
                reasons.add("final_trade_fact_count_mismatch");
            }
            if (!completion.tradeIds().equal()) {
                reasons.add("three_service_trade_id_digest_mismatch");
            }
            if (!SteadyStateRunner.balancesConverged(
                    expectedOutcome,
                    initialBuyerBalances,
                    initialSellerBalances,
                    completion.buyerBalances(),
                    completion.sellerBalances())) {
                reasons.add("asset_settlement_mismatch");
            }
            if (completion.buyBook() != expectedOutcome.unmatchedBuyOrders()
                    || completion.sellBook() != expectedOutcome.unmatchedSellOrders()) {
                reasons.add("orderbook_count_mismatch");
            }
            if (completion.activeReservations() != 0) {
                reasons.add("match_reservations_not_drained");
            }
            if (completion.finalQueueBacklog() != 0) {
                reasons.add("final_queue_backlog");
            }
            if (completion.queueReadFailures() != 0) {
                reasons.add("final_queue_metrics_read_failures");
            }
            return reasons;
        }

        private void writeStageSummary(List<StaircaseStageResult> stages) throws IOException {
            if (config.stageOutput().isBlank()) {
                return;
            }
            Path output = Path.of(config.stageOutput());
            if (output.getParent() != null) {
                Files.createDirectories(output.getParent());
            }
            try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
                writer.write("target_order_tps,offered_order_tps,accepted_response_tps,"
                        + "response_drain_tail_seconds,completed_trade_tps,offered_load_ratio,"
                        + "completion_target_ratio,completion_to_accepted_ratio,"
                        + "http_p50_upper_ms,http_p95_upper_ms,http_p99_upper_ms,"
                        + "backlog_start,backlog_end,backlog_max,backlog_slope_per_second,passed,reasons\n");
                for (StaircaseStageResult stage : stages) {
                    SteadyWindow window = stage.window();
                    writer.write(String.format(
                            java.util.Locale.ROOT,
                            "%d,%.2f,%.2f,%.4f,%.2f,%.4f,%.4f,%.4f,"
                                    + "%d,%d,%d,%d,%d,%d,%.4f,%s,\"%s\"%n",
                            stage.targetOrderTps(),
                            stage.offeredOrderTps(),
                            stage.acceptedResponseTps(),
                            stage.responseDrainTailSeconds(),
                            window.completedTradeTps(),
                            stage.offeredLoadRatio(),
                            window.completionTargetRatio(),
                            window.completionToAcceptedRatio(),
                            stage.httpP50UpperBoundMs(),
                            stage.httpP95UpperBoundMs(),
                            stage.httpP99UpperBoundMs(),
                            window.startBacklog(),
                            window.endBacklog(),
                            window.maxBacklog(),
                            window.backlogSlopePerSecond(),
                            stage.passed(),
                            String.join("|", stage.invalidReasons())));
                }
            }
        }

        private void printResult(
                List<StaircaseStageResult> stages,
                SteadyHttpCounters counters,
                SteadyCompletion completion,
                RoleBalances initialBuyerBalances,
                RoleBalances initialSellerBalances,
                int sentTrades,
                SteadyExpectedOutcome expectedOutcome,
                double trafficEndSeconds,
                double fullConvergenceSeconds,
                List<String> invalidReasons) {
            int highestSustainableOrderTps = stages.stream()
                    .filter(StaircaseStageResult::passed)
                    .mapToInt(StaircaseStageResult::targetOrderTps)
                    .max()
                    .orElse(0);
            int firstFailedOrderTps = stages.stream()
                    .filter(stage -> !stage.passed())
                    .mapToInt(StaircaseStageResult::targetOrderTps)
                    .findFirst()
                    .orElse(0);

            System.out.println("{");
            System.out.println("  \"benchmarkSchemaVersion\": 2,");
            System.out.println("  \"benchmarkContract\": \"http-matched-staircase-chain\",");
            System.out.printf("  \"runId\": \"%s\",%n", json(config.common().runId()));
            System.out.printf("  \"marketId\": \"%s\",%n", json(config.common().marketId()));
            System.out.printf("  \"runtimeProfile\": \"%s\",%n", json(config.runtimeProfile()));
            System.out.printf("  \"arrivalPattern\": \"%s\",%n",
                    config.arrivalPattern().externalName());
            System.out.printf("  \"workloadSeed\": %d,%n", config.workloadSeed());
            System.out.printf("  \"startTotalOrderTps\": %d,%n", config.startOrderTps());
            System.out.printf("  \"endTotalOrderTps\": %d,%n", config.endOrderTps());
            System.out.printf("  \"stepTotalOrderTps\": %d,%n", config.stepOrderTps());
            System.out.printf("  \"stageWarmupSeconds\": %d,%n", config.stageWarmupSeconds());
            System.out.printf("  \"stageMeasurementSeconds\": %d,%n", config.stageDurationSeconds());
            System.out.printf("  \"stopOnFailedStage\": %s,%n", config.stopOnFailedStage());
            System.out.printf("  \"stagesAttempted\": %d,%n", stages.size());
            System.out.printf("  \"highestSustainableOrderTps\": %d,%n", highestSustainableOrderTps);
            System.out.printf("  \"highestSustainableTradeTpsTarget\": %.2f,%n",
                    highestSustainableOrderTps / 2.0);
            System.out.printf("  \"firstFailedOrderTps\": %d,%n", firstFailedOrderTps);
            System.out.printf("  \"capacityLimitObserved\": %s,%n", firstFailedOrderTps > 0);
            System.out.printf("  \"sentHttpOrders\": %d,%n", sentTrades * 2L);
            System.out.printf("  \"scheduledBalancedTrades\": %d,%n", sentTrades);
            System.out.printf("  \"durableBuyOrders\": %d,%n", expectedOutcome.buyOrders());
            System.out.printf("  \"durableSellOrders\": %d,%n", expectedOutcome.sellOrders());
            System.out.printf("  \"expectedTrades\": %d,%n", expectedOutcome.pairableTrades());
            System.out.printf("  \"httpAccepted\": %d,%n", counters.accepted());
            System.out.printf("  \"http429\": %d,%n", counters.tooManyRequests.get());
            System.out.printf("  \"http503\": %d,%n", counters.unavailable.get());
            System.out.printf("  \"httpOtherFailures\": %d,%n", counters.otherFailures.get());
            System.out.printf("  \"trafficSeconds\": %.4f,%n", trafficEndSeconds);
            System.out.printf("  \"stages\": [%n");
            for (int i = 0; i < stages.size(); i++) {
                StaircaseStageResult stage = stages.get(i);
                SteadyWindow window = stage.window();
                System.out.println("    {");
                System.out.printf("      \"targetTotalOrderTps\": %d,%n", stage.targetOrderTps());
                System.out.printf("      \"targetTradeTps\": %.2f,%n", stage.targetOrderTps() / 2.0);
                System.out.printf("      \"requestSchedulingSeconds\": %.4f,%n",
                        stage.schedulingSeconds());
                System.out.printf("      \"responseCompletionSeconds\": %.4f,%n",
                        stage.responseCompletionSeconds());
                System.out.printf("      \"responseDrainTailSeconds\": %.4f,%n",
                        stage.responseDrainTailSeconds());
                System.out.printf("      \"expectedMeasuredOrders\": %d,%n", stage.expectedMeasuredOrders());
                System.out.printf("      \"httpAccepted\": %d,%n", stage.httpAccepted());
                System.out.printf("      \"offeredOrderTps\": %.2f,%n", stage.offeredOrderTps());
                System.out.printf("      \"acceptedResponseTps\": %.2f,%n",
                        stage.acceptedResponseTps());
                System.out.printf("      \"offeredLoadRatio\": %.4f,%n", stage.offeredLoadRatio());
                System.out.printf("      \"completedTradeTps\": %.2f,%n", window.completedTradeTps());
                System.out.printf("      \"completionTargetRatio\": %.4f,%n",
                        window.completionTargetRatio());
                System.out.printf("      \"completionToAcceptedRatio\": %.4f,%n",
                        window.completionToAcceptedRatio());
                System.out.printf("      \"http429\": %d,%n", stage.http429());
                System.out.printf("      \"http503\": %d,%n", stage.http503());
                System.out.printf("      \"httpOtherFailures\": %d,%n", stage.httpOtherFailures());
                System.out.printf("      \"httpLatencyP50UpperBoundMs\": %d,%n",
                        stage.httpP50UpperBoundMs());
                System.out.printf("      \"httpLatencyP95UpperBoundMs\": %d,%n",
                        stage.httpP95UpperBoundMs());
                System.out.printf("      \"httpLatencyP99UpperBoundMs\": %d,%n",
                        stage.httpP99UpperBoundMs());
                System.out.printf("      \"backlogStart\": %d,%n", window.startBacklog());
                System.out.printf("      \"backlogEnd\": %d,%n", window.endBacklog());
                System.out.printf("      \"maxBacklog\": %d,%n", window.maxBacklog());
                System.out.printf("      \"backlogSlopePerSecond\": %.4f,%n",
                        window.backlogSlopePerSecond());
                System.out.printf("      \"passed\": %s,%n", stage.passed());
                System.out.printf("      \"invalidReasons\": %s%n",
                        jsonArray(stage.invalidReasons()));
                System.out.printf("    }%s%n", i + 1 < stages.size() ? "," : "");
            }
            System.out.println("  ],");
            System.out.printf("  \"finalOrderSubmissionRows\": %d,%n", completion.submitted());
            System.out.printf("  \"finalWalletReservationRows\": %d,%n", completion.reservationClaims());
            System.out.printf("  \"finalOrderConfirmationRows\": %d,%n", completion.confirmed());
            System.out.printf("  \"finalMatchedOrderRows\": %d,%n", completion.matchedOrders());
            System.out.printf("  \"finalMatchTradeRows\": %d,%n", completion.matchTrades());
            System.out.printf("  \"finalOrderTradeRows\": %d,%n", completion.orderTrades());
            System.out.printf("  \"finalWalletTradeRows\": %d,%n", completion.walletTrades());
            System.out.printf("  \"threeServiceTradeIdsEqual\": %s,%n", completion.tradeIds().equal());
            System.out.printf("  \"tradeIdFingerprint\": \"%s\",%n",
                    completion.tradeIds().fingerprint());
            printBalances("initialBuyer", initialBuyerBalances);
            printBalances("initialSeller", initialSellerBalances);
            printBalances("finalBuyer", completion.buyerBalances());
            printBalances("finalSeller", completion.sellerBalances());
            System.out.printf("  \"remainingBuyOrders\": %d,%n", completion.buyBook());
            System.out.printf("  \"remainingSellOrders\": %d,%n", completion.sellBook());
            System.out.printf("  \"activeMatchReservations\": %d,%n", completion.activeReservations());
            System.out.printf("  \"finalQueueBacklog\": %d,%n", completion.finalQueueBacklog());
            printQueueDepths(completion.finalQueueDepths());
            System.out.printf("  \"fullConvergenceSeconds\": %.4f,%n", fullConvergenceSeconds);
            System.out.printf("  \"fullConvergenceTradeTps\": %.2f,%n",
                    expectedOutcome.pairableTrades() / Math.max(fullConvergenceSeconds, 0.001));
            System.out.printf("  \"sampleOutput\": \"%s\",%n", json(config.sampleOutput()));
            System.out.printf("  \"stageOutput\": \"%s\",%n", json(config.stageOutput()));
            System.out.printf("  \"validForCapacitySearch\": %s,%n", invalidReasons.isEmpty());
            System.out.printf("  \"capacityInvalidReasons\": %s%n", jsonArray(invalidReasons));
            System.out.println("}");
        }

        private Config withEvents(Config original, int events) {
            return new Config(
                    original.runId(),
                    original.marketId(),
                    events,
                    original.targetTps(),
                    original.usersPerSide(),
                    original.workers(),
                    original.maxInFlight(),
                    original.waitTimeoutSeconds(),
                    original.resetData(),
                    original.flushRedisOnReset(),
                    original.orderUrl(),
                    original.walletUrl(),
                    original.orderJdbcUrl(),
                    original.walletJdbcUrl(),
                    original.matchJdbcUrl(),
                    original.jdbcUser(),
                    original.jdbcPassword(),
                    original.redisHost(),
                    original.redisPort(),
                    original.rabbitManagementUrl(),
                    original.rabbitVhost(),
                    original.rabbitUser(),
                    original.rabbitPassword());
        }
    }

    private static final class SteadyHttpCounters {
        private final AtomicInteger sellAccepted = new AtomicInteger();
        private final AtomicInteger buyAccepted = new AtomicInteger();
        private final AtomicInteger tooManyRequests = new AtomicInteger();
        private final AtomicInteger unavailable = new AtomicInteger();
        private final AtomicInteger otherFailures = new AtomicInteger();
        private final AtomicInteger unscheduled = new AtomicInteger();
        private final LatencyHistogram latency = new LatencyHistogram();

        private void recordResponse(String side, int status, long latencyNanos, String body) {
            latency.record(latencyNanos);
            switch (status) {
                case 200, 202 -> {
                    if ("BUY".equals(side)) {
                        buyAccepted.incrementAndGet();
                    } else {
                        sellAccepted.incrementAndGet();
                    }
                }
                case 429 -> tooManyRequests.incrementAndGet();
                case 503 -> unavailable.incrementAndGet();
                default -> {
                    int failures = otherFailures.incrementAndGet();
                    if (failures <= 10) {
                        System.err.printf(
                                "%s steady HTTP failure status=%d body=%s%n", side, status, body);
                    }
                }
            }
        }

        private void recordFailure(String side, Throwable exception) {
            int failures = otherFailures.incrementAndGet();
            if (failures <= 10) {
                System.err.printf("%s steady HTTP request failed: %s%n", side, exception.getMessage());
            }
        }

        private void recordUnscheduled(int count) {
            unscheduled.addAndGet(count);
        }

        private long accepted() {
            return sellAccepted.get() + (long) buyAccepted.get();
        }

        private long failures() {
            return tooManyRequests.get()
                    + (long) unavailable.get()
                    + otherFailures.get()
                    + unscheduled.get();
        }
    }

    private static final class LatencyHistogram {
        private static final long[] UPPER_BOUNDS_MILLIS = {
                1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000
        };
        private final AtomicLongArray buckets = new AtomicLongArray(UPPER_BOUNDS_MILLIS.length);

        private void record(long latencyNanos) {
            long latencyMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(latencyNanos));
            int bucket = UPPER_BOUNDS_MILLIS.length - 1;
            for (int i = 0; i < UPPER_BOUNDS_MILLIS.length; i++) {
                if (latencyMillis <= UPPER_BOUNDS_MILLIS[i]) {
                    bucket = i;
                    break;
                }
            }
            buckets.incrementAndGet(bucket);
        }

        private long percentileUpperBoundMillis(double percentile) {
            long total = 0;
            for (int i = 0; i < buckets.length(); i++) {
                total += buckets.get(i);
            }
            if (total == 0) {
                return 0;
            }
            long target = (long) Math.ceil(total * percentile);
            long cumulative = 0;
            for (int i = 0; i < buckets.length(); i++) {
                cumulative += buckets.get(i);
                if (cumulative >= target) {
                    return UPPER_BOUNDS_MILLIS[i];
                }
            }
            return UPPER_BOUNDS_MILLIS[UPPER_BOUNDS_MILLIS.length - 1];
        }
    }

    record SteadySample(
            double elapsedSeconds,
            long httpAccepted,
            long httpFailures,
            long matchTrades,
            long orderTrades,
            long walletTrades,
            long completedTrades,
            long queueBacklog,
            long queueReadFailures) {
    }

    record BacklogWindow(
            long start,
            long end,
            long max,
            double slopePerSecond,
            int validSamples) {
    }

    private record SteadyWindow(
            double startSeconds,
            double endSeconds,
            double observedSeconds,
            long acceptedOrders,
            long completedTrades,
            double acceptedOrderTps,
            double completedTradeTps,
            double offeredLoadRatio,
            double completionTargetRatio,
            double completionToAcceptedRatio,
            long startBacklog,
            long endBacklog,
            long maxBacklog,
            double backlogSlopePerSecond,
            long queueReadFailures,
            int samples) {
        private static SteadyWindow empty() {
            return new SteadyWindow(
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, Double.POSITIVE_INFINITY, 0, 0);
        }
    }

    private record TradeIdDigest(long count, String fingerprint) {
    }

    private record TradeIdDigestCheck(
            boolean equal,
            long matchCount,
            long orderCount,
            long walletCount,
            String fingerprint) {
        private static TradeIdDigestCheck empty() {
            return new TradeIdDigestCheck(false, 0, 0, 0, "");
        }
    }

    record SteadyDatabaseBaseline(
            long submitted,
            long reservationClaims,
            long confirmed,
            long matchedOrders,
            long matchTrades,
            long orderTrades,
            long walletTrades) {
    }

    private record SteadyCompletion(
            long submitted,
            long reservationClaims,
            long confirmed,
            long matchedOrders,
            long matchTrades,
            long orderTrades,
            long walletTrades,
            RoleBalances buyerBalances,
            RoleBalances sellerBalances,
            long buyBook,
            long sellBook,
            long activeReservations,
            TradeIdDigestCheck tradeIds,
            long finalQueueBacklog,
            long queueReadFailures,
            Map<String, QueueDepth> finalQueueDepths,
            double elapsedSeconds) {
        private static SteadyCompletion empty() {
            return new SteadyCompletion(
                    0, 0, 0, 0, 0, 0, 0,
                    RoleBalances.empty(), RoleBalances.empty(),
                    -1, -1, -1, TradeIdDigestCheck.empty(),
                    Long.MAX_VALUE, 0, Map.of(), 0);
        }
    }

    record SteadyExpectedOutcome(long buyOrders, long sellOrders) {
        SteadyExpectedOutcome {
            if (buyOrders < 0 || sellOrders < 0) {
                throw new IllegalArgumentException("Durable accepted order counts cannot be negative");
            }
        }

        static SteadyExpectedOutcome balanced(long trades) {
            return new SteadyExpectedOutcome(trades, trades);
        }

        long acceptedOrders() {
            return buyOrders + sellOrders;
        }

        long pairableTrades() {
            return Math.min(buyOrders, sellOrders);
        }

        long unmatchedBuyOrders() {
            return buyOrders - pairableTrades();
        }

        long unmatchedSellOrders() {
            return sellOrders - pairableTrades();
        }
    }

    private record StaircasePhaseSendResult(
            int nextTradeIndex,
            double schedulingSeconds,
            double responseCompletionSeconds,
            double responseDrainTailSeconds) {
    }

    private record StaircaseStageResult(
            int targetOrderTps,
            double stageStartSeconds,
            double measurementStartSeconds,
            double measurementScheduledEndSeconds,
            double measurementResponseEndSeconds,
            double schedulingSeconds,
            double responseCompletionSeconds,
            double responseDrainTailSeconds,
            int expectedMeasuredOrders,
            long httpAccepted,
            long http429,
            long http503,
            long httpOtherFailures,
            long httpP50UpperBoundMs,
            long httpP95UpperBoundMs,
            long httpP99UpperBoundMs,
            SteadyWindow window,
            double maxBacklogGrowthPerSecond,
            long maxSteadyBacklog,
            List<String> invalidReasons) {

        private double offeredOrderTps() {
            return expectedMeasuredOrders / Math.max(schedulingSeconds, 0.001);
        }

        private double acceptedResponseTps() {
            return httpAccepted / Math.max(responseCompletionSeconds, 0.001);
        }

        private double offeredLoadRatio() {
            return offeredOrderTps() / targetOrderTps;
        }

        private boolean passed() {
            return invalidReasons.isEmpty();
        }
    }

    private record StaircaseConfig(
            Config common,
            int startOrderTps,
            int endOrderTps,
            int stepOrderTps,
            int stageWarmupSeconds,
            int stageDurationSeconds,
            int sampleIntervalSeconds,
            int progressIntervalSeconds,
            double minOfferedLoadRatio,
            double minCompletionRatio,
            double configuredMaxBacklogGrowthPerSecond,
            long configuredMaxSteadyBacklog,
            boolean stopOnFailedStage,
            BalancedOrderSchedule.ArrivalPattern arrivalPattern,
            long workloadSeed,
            String runtimeProfile,
            String sampleOutput,
            String stageOutput) {

        private static StaircaseConfig from(String[] args) {
            int startOrderTps = SteadyStateConfig.intArg(args, "--start-order-tps", 100);
            int endOrderTps = SteadyStateConfig.intArg(args, "--end-order-tps", 2_000);
            int stepOrderTps = SteadyStateConfig.intArg(args, "--step-order-tps", 100);
            int stageWarmupSeconds = SteadyStateConfig.intArg(args, "--stage-warmup-seconds", 30);
            int stageDurationSeconds = SteadyStateConfig.intArg(args, "--stage-duration-seconds", 60);
            if (startOrderTps <= 0
                    || endOrderTps < startOrderTps
                    || stepOrderTps <= 0
                    || startOrderTps % 2 != 0
                    || endOrderTps % 2 != 0
                    || stepOrderTps % 2 != 0) {
                throw new IllegalArgumentException(
                        "staircase order TPS values must be positive even numbers with end >= start");
            }
            if ((endOrderTps - startOrderTps) % stepOrderTps != 0) {
                throw new IllegalArgumentException(
                        "--end-order-tps must be reachable exactly from start using the configured step");
            }
            if (stageWarmupSeconds < 0 || stageDurationSeconds < 2) {
                throw new IllegalArgumentException(
                        "stage warmup must be non-negative and stage duration must be at least 2 seconds");
            }

            long maximumOrders = 0;
            for (int target = startOrderTps; target <= endOrderTps; target += stepOrderTps) {
                maximumOrders = Math.addExact(
                        maximumOrders,
                        Math.multiplyExact(
                                (long) target,
                                stageWarmupSeconds + (long) stageDurationSeconds));
            }
            int maximumTrades = Math.toIntExact(maximumOrders / 2);
            Config parsed = Config.from(args);
            Config common = new Config(
                    parsed.runId(),
                    parsed.marketId(),
                    maximumTrades,
                    endOrderTps,
                    parsed.usersPerSide(),
                    parsed.workers(),
                    parsed.maxInFlight(),
                    parsed.waitTimeoutSeconds(),
                    parsed.resetData(),
                    parsed.flushRedisOnReset(),
                    parsed.orderUrl(),
                    parsed.walletUrl(),
                    parsed.orderJdbcUrl(),
                    parsed.walletJdbcUrl(),
                    parsed.matchJdbcUrl(),
                    parsed.jdbcUser(),
                    parsed.jdbcPassword(),
                    parsed.redisHost(),
                    parsed.redisPort(),
                    parsed.rabbitManagementUrl(),
                    parsed.rabbitVhost(),
                    parsed.rabbitUser(),
                    parsed.rabbitPassword());
            int sampleIntervalSeconds =
                    SteadyStateConfig.intArg(args, "--sample-interval-seconds", 1);
            int progressIntervalSeconds =
                    SteadyStateConfig.intArg(args, "--progress-interval-seconds", 10);
            if (sampleIntervalSeconds <= 0 || progressIntervalSeconds <= 0) {
                throw new IllegalArgumentException("sample and progress intervals must be positive");
            }
            return new StaircaseConfig(
                    common,
                    startOrderTps,
                    endOrderTps,
                    stepOrderTps,
                    stageWarmupSeconds,
                    stageDurationSeconds,
                    sampleIntervalSeconds,
                    progressIntervalSeconds,
                    SteadyStateConfig.doubleArg(args, "--min-offered-load-ratio", 0.95),
                    SteadyStateConfig.doubleArg(args, "--min-completion-ratio", 0.95),
                    SteadyStateConfig.doubleArg(args, "--max-backlog-growth-per-second", -1),
                    SteadyStateConfig.longArg(args, "--max-steady-backlog", -1),
                    Boolean.parseBoolean(
                            SteadyStateConfig.stringArg(args, "--stop-on-failed-stage", "true")),
                    BalancedOrderSchedule.ArrivalPattern.parse(
                            SteadyStateConfig.stringArg(args, "--arrival-pattern", "shuffled")),
                    SteadyStateConfig.longArg(args, "--workload-seed", 20260804L),
                    SteadyStateConfig.stringArg(args, "--runtime-profile", "canonical"),
                    SteadyStateConfig.stringArg(args, "--sample-output", ""),
                    SteadyStateConfig.stringArg(args, "--stage-output", ""));
        }

        private List<Integer> targets() {
            List<Integer> targets = new ArrayList<>();
            for (int target = startOrderTps; target <= endOrderTps; target += stepOrderTps) {
                targets.add(target);
            }
            return targets;
        }

        private double maxBacklogGrowthPerSecond(int targetOrderTps) {
            return configuredMaxBacklogGrowthPerSecond >= 0
                    ? configuredMaxBacklogGrowthPerSecond
                    : Math.max(1.0, targetOrderTps * 0.01);
        }

        private long maxSteadyBacklog(int targetOrderTps) {
            return configuredMaxSteadyBacklog >= 0
                    ? configuredMaxSteadyBacklog
                    : targetOrderTps * 30L;
        }

        private SteadyStateConfig asSteadyConfig() {
            return new SteadyStateConfig(
                    common,
                    endOrderTps,
                    stageWarmupSeconds,
                    stageDurationSeconds,
                    sampleIntervalSeconds,
                    progressIntervalSeconds,
                    minOfferedLoadRatio,
                    minCompletionRatio,
                    maxBacklogGrowthPerSecond(endOrderTps),
                    maxSteadyBacklog(endOrderTps),
                    arrivalPattern,
                    workloadSeed,
                    runtimeProfile,
                    "legacy-sync",
                    sampleOutput);
        }
    }

    private record SteadyStateConfig(
            Config common,
            int targetOrderTps,
            int warmupSeconds,
            int durationSeconds,
            int sampleIntervalSeconds,
            int progressIntervalSeconds,
            double minOfferedLoadRatio,
            double minCompletionRatio,
            double maxBacklogGrowthPerSecond,
            long maxSteadyBacklog,
            BalancedOrderSchedule.ArrivalPattern arrivalPattern,
            long workloadSeed,
            String runtimeProfile,
            String httpDriverMode,
            String sampleOutput) {

        private static SteadyStateConfig from(String[] args) {
            int targetOrderTps = intArg(args, "--target-order-tps", 300);
            int warmupSeconds = intArg(args, "--warmup-seconds", 60);
            int durationSeconds = intArg(args, "--duration-seconds", 1_800);
            if (targetOrderTps <= 0 || targetOrderTps % 2 != 0) {
                throw new IllegalArgumentException("--target-order-tps must be a positive even number");
            }
            if (warmupSeconds < 0 || durationSeconds < 2) {
                throw new IllegalArgumentException("warmup must be non-negative and duration must be at least 2 seconds");
            }
            int totalSeconds = Math.addExact(warmupSeconds, durationSeconds);
            int totalOrders = Math.multiplyExact(targetOrderTps, totalSeconds);
            int expectedTrades = totalOrders / 2;
            Config parsed = Config.from(args);
            Config common = new Config(
                    parsed.runId(),
                    parsed.marketId(),
                    expectedTrades,
                    targetOrderTps,
                    parsed.usersPerSide(),
                    parsed.workers(),
                    parsed.maxInFlight(),
                    parsed.waitTimeoutSeconds(),
                    parsed.resetData(),
                    parsed.flushRedisOnReset(),
                    parsed.orderUrl(),
                    parsed.walletUrl(),
                    parsed.orderJdbcUrl(),
                    parsed.walletJdbcUrl(),
                    parsed.matchJdbcUrl(),
                    parsed.jdbcUser(),
                    parsed.jdbcPassword(),
                    parsed.redisHost(),
                    parsed.redisPort(),
                    parsed.rabbitManagementUrl(),
                    parsed.rabbitVhost(),
                    parsed.rabbitUser(),
                    parsed.rabbitPassword());
            int sampleIntervalSeconds = intArg(args, "--sample-interval-seconds", 1);
            int progressIntervalSeconds = intArg(args, "--progress-interval-seconds", 10);
            if (sampleIntervalSeconds <= 0 || progressIntervalSeconds <= 0) {
                throw new IllegalArgumentException("sample and progress intervals must be positive");
            }
            String httpDriverMode = stringArg(args, "--http-driver-mode", "legacy-sync");
            if (!PreparedHttpLoadDriver.MODE.equals(httpDriverMode)
                    && !"legacy-sync".equals(httpDriverMode)
                    && !"external-vegeta".equals(httpDriverMode)) {
                throw new IllegalArgumentException(
                        "--http-driver-mode must be prepared-sync, legacy-sync, or external-vegeta");
            }
            return new SteadyStateConfig(
                    common,
                    targetOrderTps,
                    warmupSeconds,
                    durationSeconds,
                    sampleIntervalSeconds,
                    progressIntervalSeconds,
                    doubleArg(args, "--min-offered-load-ratio", 0.95),
                    doubleArg(args, "--min-completion-ratio", 0.95),
                    doubleArg(
                            args,
                            "--max-backlog-growth-per-second",
                            Math.max(1.0, targetOrderTps * 0.01)),
                    longArg(args, "--max-steady-backlog", targetOrderTps * 30L),
                    BalancedOrderSchedule.ArrivalPattern.parse(
                            stringArg(args, "--arrival-pattern", "shuffled")),
                    longArg(args, "--workload-seed", 20260804L),
                    stringArg(args, "--runtime-profile", "canonical"),
                    httpDriverMode,
                    stringArg(args, "--sample-output", ""));
        }

        private boolean usesPreparedDriver() {
            return PreparedHttpLoadDriver.MODE.equals(httpDriverMode);
        }

        private static int intArg(String[] args, String name, int defaultValue) {
            return Integer.parseInt(stringArg(args, name, String.valueOf(defaultValue)));
        }

        private static long longArg(String[] args, String name, long defaultValue) {
            return Long.parseLong(stringArg(args, name, String.valueOf(defaultValue)));
        }

        private static double doubleArg(String[] args, String name, double defaultValue) {
            return Double.parseDouble(stringArg(args, name, String.valueOf(defaultValue)));
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

    private record BuyRequest(UUID orderId, int bidPrice, int amount, UUID bidder) {
    }

    private record SellRequest(UUID orderId, int sellPrice, int amount, UUID seller) {
    }

    private record HttpPhaseResult(
            int accepted,
            int tooManyRequests,
            int unavailable,
            int otherFailures,
            double elapsedSeconds,
            double p50Millis,
            double p95Millis,
            double p99Millis) {
    }

    private record SellAdmissionResult(
            long submitted,
            long reservationClaims,
            long confirmed,
            long sellBook,
            long finalQueueBacklog,
            long queueReadFailures,
            long maxQueueBacklog,
            double elapsedSeconds) {
        static SellAdmissionResult empty() {
            return new SellAdmissionResult(0, 0, 0, 0, Long.MAX_VALUE, 0, 0, 0);
        }
    }

    private record RoleBalances(
            long availableAmount,
            long lockedAmount,
            long availableCurrency,
            long lockedCurrency) {
        static RoleBalances empty() {
            return new RoleBalances(0, 0, 0, 0);
        }
    }

    private record TradeIdSetCheck(
            boolean equal,
            int matchCount,
            int orderCount,
            int walletCount,
            int unionCount,
            String fingerprint) {
        static TradeIdSetCheck empty() {
            return new TradeIdSetCheck(false, 0, 0, 0, 0, "");
        }
    }

    private record CompletionResult(
            long submitted,
            long reservationClaims,
            long confirmed,
            long matchedOrders,
            long matchTrades,
            long orderTrades,
            long walletTrades,
            RoleBalances buyerBalances,
            RoleBalances sellerBalances,
            long buyBook,
            long sellBook,
            long activeReservations,
            TradeIdSetCheck tradeIds,
            long finalQueueBacklog,
            long queueReadFailures,
            long maxQueueBacklog,
            Map<String, QueueDepth> finalQueueDepths,
            double elapsedSeconds,
            double postCompletionVerificationSeconds) {
        static CompletionResult empty() {
            return new CompletionResult(
                    0, 0, 0, 0, 0, 0, 0,
                    RoleBalances.empty(), RoleBalances.empty(),
                    -1, -1, -1, TradeIdSetCheck.empty(),
                    Long.MAX_VALUE, 0, 0, Map.of(), 0, 0);
        }
    }

    private record DatabaseHandles(Connection order, Connection wallet, Connection match)
            implements AutoCloseable {
        static DatabaseHandles open(Config config) throws Exception {
            Connection order = DriverManager.getConnection(
                    config.orderJdbcUrl(), config.jdbcUser(), config.jdbcPassword());
            try {
                Connection wallet = DriverManager.getConnection(
                        config.walletJdbcUrl(), config.jdbcUser(), config.jdbcPassword());
                try {
                    Connection match = DriverManager.getConnection(
                            config.matchJdbcUrl(), config.jdbcUser(), config.jdbcPassword());
                    return new DatabaseHandles(order, wallet, match);
                } catch (Exception e) {
                    wallet.close();
                    throw e;
                }
            } catch (Exception e) {
                order.close();
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            Exception failure = null;
            for (Connection connection : List.of(match, wallet, order)) {
                try {
                    connection.close();
                } catch (Exception e) {
                    failure = failure == null ? e : failure;
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private record Config(
            String runId,
            String marketId,
            int events,
            int targetTps,
            int usersPerSide,
            int workers,
            int maxInFlight,
            int waitTimeoutSeconds,
            boolean resetData,
            boolean flushRedisOnReset,
            String orderUrl,
            String walletUrl,
            String orderJdbcUrl,
            String walletJdbcUrl,
            String matchJdbcUrl,
            String jdbcUser,
            String jdbcPassword,
            String redisHost,
            int redisPort,
            String rabbitManagementUrl,
            String rabbitVhost,
            String rabbitUser,
            String rabbitPassword) {

        static Config from(String[] args) {
            int workers = intArg(args, "--workers", 128);
            return new Config(
                    stringArg(args, "--run-id", "HTTP_MATCHED_" + Instant.now().toEpochMilli()),
                    stringArg(args, "--market-id", HTTP_MARKET_ID),
                    intArg(args, "--events", 10_000),
                    intArg(args, "--target-tps", 2_000),
                    intArg(args, "--users-per-side", 500),
                    workers,
                    intArg(args, "--max-in-flight", workers * 2),
                    intArg(args, "--wait-timeout-seconds", 300),
                    booleanArg(args, "--reset-data", true),
                    booleanArg(args, "--flush-redis-on-reset", true),
                    stringArg(args, "--order-url", "http://localhost:8080/eap-order"),
                    stringArg(args, "--wallet-url", "http://localhost:8081/eap-wallet"),
                    stringArg(args, "--order-jdbc-url", "jdbc:postgresql://localhost:15432/eap_order_db"),
                    stringArg(args, "--wallet-jdbc-url", "jdbc:postgresql://localhost:15433/eap_wallet_db"),
                    stringArg(args, "--match-jdbc-url", "jdbc:postgresql://localhost:15434/eap_match_db"),
                    stringArg(args, "--jdbc-user", "admin"),
                    stringArg(args, "--jdbc-password",
                            environmentOrDefault("EAP_LOADTEST_JDBC_PASSWORD", "admin123")),
                    stringArg(args, "--redis-host", "localhost"),
                    intArg(args, "--redis-port", 6379),
                    stringArg(args, "--rabbit-management-url", "http://localhost:15672"),
                    stringArg(args, "--rabbit-vhost",
                            environmentOrDefault("EAP_LOADTEST_RABBIT_VHOST", "/")),
                    stringArg(args, "--rabbit-user", "admin"),
                    stringArg(args, "--rabbit-password",
                            environmentOrDefault("EAP_LOADTEST_RABBIT_PASSWORD", "admin123")));
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

        private static String environmentOrDefault(String name, String defaultValue) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? defaultValue : value;
        }
    }
}
