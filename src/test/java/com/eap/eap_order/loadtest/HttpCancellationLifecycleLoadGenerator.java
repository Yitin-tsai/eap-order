package com.eap.eap_order.loadtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.eap.common.constants.RabbitMQConstants.DEAD_LETTER_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.MATCH_ENGINE_ORDER_CANCELLATION_REQUESTED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.MATCH_ENGINE_ORDER_CONFIRMED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_CANCELLATION_RESULT_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_CONFIRMED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_FAILED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_TRADE_EXECUTED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_ORDER_CANCELLATION_RESULT_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_ORDER_SUBMITTED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_TRADE_EXECUTED_QUEUE;

/**
 * Deterministic cross-service cancellation lifecycle verification.
 * This is a correctness contract, not a capacity benchmark.
 */
public final class HttpCancellationLifecycleLoadGenerator {

    private static final int PRICE = 100;
    private static final String MARKET_ID = "ENERGY-SPOT";
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
            DEAD_LETTER_QUEUE);

    private HttpCancellationLifecycleLoadGenerator() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.from(args);
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        HttpClient http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        RabbitManagementClient rabbit = new RabbitManagementClient(
                config.rabbitManagementUrl(),
                config.rabbitVhost(),
                config.rabbitUser(),
                config.rabbitPassword(),
                http,
                mapper);

        UUID openBuyer = registerUser(config, http, mapper);
        UUID partialSeller = registerUser(config, http, mapper);
        UUID matchingBuyer = registerUser(config, http, mapper);
        UUID openBuyOrder = stableId(config.runId(), "open-buy");
        UUID partialSellOrder = stableId(config.runId(), "partial-sell");
        UUID matchingBuyOrder = stableId(config.runId(), "matching-buy");

        try (Connection order = open(config.orderJdbcUrl(), config);
             Connection wallet = open(config.walletJdbcUrl(), config);
             Connection match = open(config.matchJdbcUrl(), config)) {
            WalletBalance openBuyerInitial = walletBalance(wallet, openBuyer);
            WalletBalance sellerInitial = walletBalance(wallet, partialSeller);
            WalletBalance matchingBuyerInitial = walletBalance(wallet, matchingBuyer);

            runOpenOrderCancellation(
                    config, http, mapper, order, wallet, match,
                    openBuyer, openBuyOrder, openBuyerInitial);
            PartialScenario partial = runPartialRemainderCancellation(
                    config, http, mapper, order, wallet, match,
                    partialSeller, matchingBuyer, partialSellOrder, matchingBuyOrder,
                    sellerInitial, matchingBuyerInitial);
            RaceSummary race = runMatchCancellationRaces(
                    config, http, mapper, order, wallet, match);

            await("all cancellation queues and DLQ drained", config.timeoutSeconds(), () -> {
                RabbitManagementClient.QueueSnapshot queues = rabbit.readQueues(CHAIN_QUEUES);
                return queues.readFailures() == 0 && queues.backlog() == 0;
            });
            RabbitManagementClient.QueueSnapshot queues = rabbit.readQueues(CHAIN_QUEUES);

            long orderOutboxDebt = scalarLong(order,
                    "SELECT count(*) FROM order_service.order_event_outbox WHERE status <> 'SENT'");
            long walletOutboxDebt = scalarLong(wallet,
                    "SELECT count(*) FROM wallet_service.outbox WHERE status <> 'SENT'");
            long matchOutboxDebt = scalarLong(match,
                    "SELECT count(*) FROM match_engine.trade_outbox WHERE status <> 'SENT'");
            long cancellationInboxApplied = scalarLong(order, """
                    SELECT count(*)
                    FROM order_service.order_cancellation_result_inbox
                    WHERE status = 'APPLIED'
                    """);
            long walletCancellationApplications = scalarLong(wallet,
                    "SELECT count(*) FROM wallet_service.order_cancellation_applications");
            if (orderOutboxDebt != 0 || walletOutboxDebt != 0 || matchOutboxDebt != 0) {
                throw new IllegalStateException("outbox debt remains: order=" + orderOutboxDebt
                        + ", wallet=" + walletOutboxDebt + ", match=" + matchOutboxDebt);
            }
            long expectedCancellationResults = 2L
                    + config.raceIterations()
                    + race.cleanupCancellations();
            if (cancellationInboxApplied != expectedCancellationResults) {
                throw new IllegalStateException(
                        "unexpected applied Order cancellation inbox rows: expected="
                                + expectedCancellationResults + ", actual=" + cancellationInboxApplied);
            }
            long expectedWalletApplications = 2L + race.cancellationWins() + race.cleanupCancellations();
            if (walletCancellationApplications != expectedWalletApplications) {
                throw new IllegalStateException(
                        "unexpected Wallet cancellation applications: expected="
                                + expectedWalletApplications + ", actual=" + walletCancellationApplications);
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("benchmarkContract", "http-cancellation-lifecycle");
            result.put("runId", config.runId());
            result.put("marketId", MARKET_ID);
            result.put("capacityEvidence", false);
            result.put("scenarios", List.of(
                    "OPEN_ORDER_CANCEL",
                    "PARTIAL_REMAINDER_CANCEL",
                    "MATCH_CANCEL_RACE"));
            result.put("openOrderCancelledAmount", 3);
            result.put("partialTradeId", partial.tradeId());
            result.put("partialMatchedAmount", 1);
            result.put("partialCancelledAmount", 1);
            result.put("threeServiceTradeIdsEqual", partial.threeServiceTradeIdsEqual());
            result.put("raceIterations", config.raceIterations());
            result.put("raceCancellationWins", race.cancellationWins());
            result.put("raceMatchingWins", race.matchingWins());
            result.put("raceCleanupCancellations", race.cleanupCancellations());
            result.put("raceMutualExclusionValid", true);
            result.put("orderCancellationInboxApplied", cancellationInboxApplied);
            result.put("walletCancellationApplications", walletCancellationApplications);
            result.put("orderOutboxDebt", orderOutboxDebt);
            result.put("walletOutboxDebt", walletOutboxDebt);
            result.put("matchOutboxDebt", matchOutboxDebt);
            result.put("finalQueueBacklog", queues.backlog());
            result.put("queueMetricsReadFailures", queues.readFailures());
            result.put("finalQueues", queues.depths());
            result.put("valid", true);
            System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
        }
    }

    private static void runOpenOrderCancellation(
            Config config,
            HttpClient http,
            ObjectMapper mapper,
            Connection order,
            Connection wallet,
            Connection match,
            UUID buyerId,
            UUID orderId,
            WalletBalance initial) throws Exception {
        placeOrder(config, http, mapper, "BUY", orderId, buyerId, 3);
        await("open BUY admitted to Wallet, Order, and Match", config.timeoutSeconds(), () -> {
            OrderState state = orderState(order, orderId);
            return walletOrderClaimed(wallet, orderId, buyerId)
                    && state != null
                    && state.remainingAmount() == 3
                    && "OPEN".equals(state.status())
                    && visibleAmount(config, http, mapper, buyerId, orderId) == 3;
        });

        cancelOrder(config, http, mapper, orderId, buyerId);
        await("open BUY cancellation converged across three services", config.timeoutSeconds(), () -> {
            CancellationDecision decision = cancellationDecision(match, orderId);
            CancellationApplication application = cancellationApplication(wallet, orderId);
            OrderState state = orderState(order, orderId);
            WalletBalance balance = walletBalance(wallet, buyerId);
            return decision != null
                    && "CANCELLED".equals(decision.status())
                    && decision.cancelledAmount() == 3
                    && application != null
                    && application.cancelledQuantity() == 3
                    && state != null
                    && "CANCELLED".equals(state.status())
                    && state.remainingAmount() == 3
                    && state.matchedAmount() == 0
                    && balance.equals(initial)
                    && visibleAmount(config, http, mapper, buyerId, orderId) == -1
                    && tradeCount(match, orderId) == 0;
        });
    }

    private static PartialScenario runPartialRemainderCancellation(
            Config config,
            HttpClient http,
            ObjectMapper mapper,
            Connection order,
            Connection wallet,
            Connection match,
            UUID sellerId,
            UUID buyerId,
            UUID sellerOrderId,
            UUID buyerOrderId,
            WalletBalance sellerInitial,
            WalletBalance buyerInitial) throws Exception {
        placeOrder(config, http, mapper, "SELL", sellerOrderId, sellerId, 2);
        await("SELL admitted before partial match", config.timeoutSeconds(), () ->
                visibleAmount(config, http, mapper, sellerId, sellerOrderId) == 2);
        placeOrder(config, http, mapper, "BUY", buyerOrderId, buyerId, 1);

        await("one trade settled and SELL remainder became visible", config.timeoutSeconds(), () -> {
            OrderState sellerState = orderState(order, sellerOrderId);
            String tradeId = singleTradeId(match, sellerOrderId);
            return tradeId != null
                    && tradeCount(order, "order_service.order_trade_applications", sellerOrderId) == 1
                    && hasTradeId(wallet, "wallet_service.trade_settlements", tradeId)
                    && sellerState != null
                    && sellerState.remainingAmount() == 1
                    && sellerState.matchedAmount() == 1
                    && "PARTIALLY_MATCHED".equals(sellerState.status())
                    && visibleAmount(config, http, mapper, sellerId, sellerOrderId) == 1;
        });

        cancelOrder(config, http, mapper, sellerOrderId, sellerId);
        await("partial SELL remainder cancellation converged", config.timeoutSeconds(), () -> {
            CancellationDecision decision = cancellationDecision(match, sellerOrderId);
            CancellationApplication application = cancellationApplication(wallet, sellerOrderId);
            OrderState sellerState = orderState(order, sellerOrderId);
            WalletBalance sellerBalance = walletBalance(wallet, sellerId);
            WalletBalance buyerBalance = walletBalance(wallet, buyerId);
            return decision != null
                    && "CANCELLED".equals(decision.status())
                    && decision.cancelledAmount() == 1
                    && application != null
                    && application.cancelledQuantity() == 1
                    && sellerState != null
                    && "CANCELLED".equals(sellerState.status())
                    && sellerState.remainingAmount() == 1
                    && sellerState.matchedAmount() == 1
                    && sellerBalance.equals(new WalletBalance(
                            sellerInitial.availableAmount() - 1,
                            sellerInitial.lockedAmount(),
                            sellerInitial.availableCurrency() + PRICE,
                            sellerInitial.lockedCurrency()))
                    && buyerBalance.equals(new WalletBalance(
                            buyerInitial.availableAmount() + 1,
                            buyerInitial.lockedAmount(),
                            buyerInitial.availableCurrency() - PRICE,
                            buyerInitial.lockedCurrency()))
                    && visibleAmount(config, http, mapper, sellerId, sellerOrderId) == -1;
        });

        Set<String> matchIds = tradeIds(match,
                "SELECT trade_id FROM match_engine.trade_executions WHERE buyer_order_id = ? OR seller_order_id = ?",
                sellerOrderId);
        Set<String> orderIds = tradeIds(order,
                "SELECT trade_id FROM order_service.order_trade_applications WHERE buyer_order_id = ? OR seller_order_id = ?",
                sellerOrderId);
        // Wallet settlement no longer duplicates order IDs, so scope its one fact by the Match trade ID.
        Set<String> walletIds = tradeIdsByExpected(wallet, matchIds);
        if (matchIds.size() != 1 || !matchIds.equals(orderIds) || !matchIds.equals(walletIds)) {
            throw new IllegalStateException("trade IDs differ: match=" + matchIds
                    + ", order=" + orderIds + ", wallet=" + walletIds);
        }
        return new PartialScenario(matchIds.iterator().next(), true);
    }

    private static RaceSummary runMatchCancellationRaces(
            Config config,
            HttpClient http,
            ObjectMapper mapper,
            Connection order,
            Connection wallet,
            Connection match) throws Exception {
        int cancellationWins = 0;
        int matchingWins = 0;
        int cleanupCancellations = 0;

        for (int iteration = 0; iteration < config.raceIterations(); iteration++) {
            UUID sellerId = registerUser(config, http, mapper);
            UUID buyerId = registerUser(config, http, mapper);
            UUID sellerOrderId = stableId(config.runId(), "race-sell-" + iteration);
            UUID buyerOrderId = stableId(config.runId(), "race-buy-" + iteration);
            WalletBalance sellerInitial = walletBalance(wallet, sellerId);
            WalletBalance buyerInitial = walletBalance(wallet, buyerId);

            placeOrder(config, http, mapper, "SELL", sellerOrderId, sellerId, 1);
            await("race SELL admitted before iteration " + iteration, config.timeoutSeconds(), () -> {
                OrderState state = orderState(order, sellerOrderId);
                return walletOrderClaimed(wallet, sellerOrderId, sellerId)
                        && state != null
                        && state.remainingAmount() == 1
                        && "OPEN".equals(state.status())
                        && visibleAmount(config, http, mapper, sellerId, sellerOrderId) == 1;
            });

            runConcurrently(
                    () -> placeOrder(config, http, mapper, "BUY", buyerOrderId, buyerId, 1),
                    () -> cancelOrder(config, http, mapper, sellerOrderId, sellerId));

            await("terminal match/cancel decision for iteration " + iteration,
                    config.timeoutSeconds(), () -> {
                        CancellationDecision decision = cancellationDecision(match, sellerOrderId);
                        return decision != null && decision.terminal();
                    });
            CancellationDecision decision = cancellationDecision(match, sellerOrderId);
            if ("CANCELLED".equals(decision.status())) {
                cancellationWins++;
                awaitCancellationWonRace(
                        config, http, mapper, order, wallet, match,
                        sellerId, buyerId, sellerOrderId, buyerOrderId,
                        sellerInitial);

                cancelOrder(config, http, mapper, buyerOrderId, buyerId);
                cleanupCancellations++;
                await("race BUY cleanup converged for iteration " + iteration,
                        config.timeoutSeconds(), () -> {
                            CancellationDecision buyerDecision = cancellationDecision(match, buyerOrderId);
                            CancellationApplication buyerApplication =
                                    cancellationApplication(wallet, buyerOrderId);
                            OrderState buyerState = orderState(order, buyerOrderId);
                            return buyerDecision != null
                                    && "CANCELLED".equals(buyerDecision.status())
                                    && buyerDecision.cancelledAmount() == 1
                                    && buyerApplication != null
                                    && buyerApplication.cancelledQuantity() == 1
                                    && buyerState != null
                                    && buyerState.remainingAmount() == 1
                                    && buyerState.matchedAmount() == 0
                                    && "CANCELLED".equals(buyerState.status())
                                    && walletBalance(wallet, buyerId).equals(buyerInitial)
                                    && visibleAmount(config, http, mapper, buyerId, buyerOrderId) == -1;
                        });
            } else if ("ALREADY_MATCHED".equals(decision.status())) {
                matchingWins++;
                awaitMatchingWonRace(
                        config, http, mapper, order, wallet, match,
                        sellerId, buyerId, sellerOrderId, buyerOrderId,
                        sellerInitial, buyerInitial);
            } else {
                throw new IllegalStateException("unexpected match/cancel race outcome: orderId="
                        + sellerOrderId + ", status=" + decision.status());
            }
        }
        return new RaceSummary(cancellationWins, matchingWins, cleanupCancellations);
    }

    private static void awaitCancellationWonRace(
            Config config,
            HttpClient http,
            ObjectMapper mapper,
            Connection order,
            Connection wallet,
            Connection match,
            UUID sellerId,
            UUID buyerId,
            UUID sellerOrderId,
            UUID buyerOrderId,
            WalletBalance sellerInitial) throws Exception {
        await("cancellation-won race convergence", config.timeoutSeconds(), () -> {
            CancellationDecision decision = cancellationDecision(match, sellerOrderId);
            CancellationApplication sellerApplication = cancellationApplication(wallet, sellerOrderId);
            OrderState sellerState = orderState(order, sellerOrderId);
            OrderState buyerState = orderState(order, buyerOrderId);
            return decision != null
                    && "CANCELLED".equals(decision.status())
                    && decision.cancelledAmount() == 1
                    && tradeCount(match, sellerOrderId) == 0
                    && tradeCount(match, buyerOrderId) == 0
                    && sellerApplication != null
                    && sellerApplication.cancelledQuantity() == 1
                    && sellerState != null
                    && sellerState.remainingAmount() == 1
                    && sellerState.matchedAmount() == 0
                    && "CANCELLED".equals(sellerState.status())
                    && walletBalance(wallet, sellerId).equals(sellerInitial)
                    && visibleAmount(config, http, mapper, sellerId, sellerOrderId) == -1
                    && walletOrderClaimed(wallet, buyerOrderId, buyerId)
                    && cancellationApplication(wallet, buyerOrderId) == null
                    && buyerState != null
                    && buyerState.remainingAmount() == 1
                    && buyerState.matchedAmount() == 0
                    && "OPEN".equals(buyerState.status())
                    && visibleAmount(config, http, mapper, buyerId, buyerOrderId) == 1;
        });
    }

    private static void awaitMatchingWonRace(
            Config config,
            HttpClient http,
            ObjectMapper mapper,
            Connection order,
            Connection wallet,
            Connection match,
            UUID sellerId,
            UUID buyerId,
            UUID sellerOrderId,
            UUID buyerOrderId,
            WalletBalance sellerInitial,
            WalletBalance buyerInitial) throws Exception {
        await("matching-won race convergence", config.timeoutSeconds(), () -> {
            String tradeId = singleTradeId(match, sellerOrderId);
            OrderState sellerState = orderState(order, sellerOrderId);
            OrderState buyerState = orderState(order, buyerOrderId);
            return tradeId != null
                    && tradeCount(match, buyerOrderId) == 1
                    && hasTradeId(order, "order_service.order_trade_applications", tradeId)
                    && hasTradeId(wallet, "wallet_service.trade_settlements", tradeId)
                    && cancellationApplication(wallet, sellerOrderId) == null
                    && cancellationApplication(wallet, buyerOrderId) == null
                    && sellerState != null
                    && buyerState != null
                    && sellerState.remainingAmount() == 0
                    && buyerState.remainingAmount() == 0
                    && sellerState.matchedAmount() == 1
                    && buyerState.matchedAmount() == 1
                    && "MATCHED".equals(sellerState.status())
                    && "MATCHED".equals(buyerState.status())
                    && walletBalance(wallet, sellerId).equals(new WalletBalance(
                            sellerInitial.availableAmount() - 1,
                            sellerInitial.lockedAmount(),
                            sellerInitial.availableCurrency() + PRICE,
                            sellerInitial.lockedCurrency()))
                    && walletBalance(wallet, buyerId).equals(new WalletBalance(
                            buyerInitial.availableAmount() + 1,
                            buyerInitial.lockedAmount(),
                            buyerInitial.availableCurrency() - PRICE,
                            buyerInitial.lockedCurrency()))
                    && visibleAmount(config, http, mapper, sellerId, sellerOrderId) == -1
                    && visibleAmount(config, http, mapper, buyerId, buyerOrderId) == -1;
        });
    }

    private static void runConcurrently(CheckedAction first, CheckedAction second) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<?> firstResult = executor.submit(() -> {
                start.await();
                first.run();
                return null;
            });
            Future<?> secondResult = executor.submit(() -> {
                start.await();
                second.run();
                return null;
            });
            start.countDown();
            firstResult.get();
            secondResult.get();
        } finally {
            executor.shutdownNow();
        }
    }

    private static UUID registerUser(Config config, HttpClient http, ObjectMapper mapper) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder()
                        .uri(URI.create(config.walletUrl() + "/v1/wallet/register"))
                        .timeout(Duration.ofSeconds(10))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        requireStatus(response, 200, "wallet registration");
        return UUID.fromString(mapper.readTree(response.body()).path("userId").asText());
    }

    private static void placeOrder(
            Config config,
            HttpClient http,
            ObjectMapper mapper,
            String side,
            UUID orderId,
            UUID userId,
            int amount) throws Exception {
        Map<String, Object> body = "BUY".equals(side)
                ? Map.of("orderId", orderId, "bidPrice", PRICE, "amount", amount, "bidder", userId)
                : Map.of("orderId", orderId, "sellPrice", PRICE, "amount", amount, "seller", userId);
        HttpResponse<String> response = http.send(HttpRequest.newBuilder()
                        .uri(URI.create(config.orderUrl() + "/bid/" + side.toLowerCase()))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        requireStatus(response, 200, side + " order submission");
        UUID returnedOrderId = UUID.fromString(mapper.readTree(response.body()).path("orderId").asText());
        if (!orderId.equals(returnedOrderId)) {
            throw new IllegalStateException("Order HTTP changed caller orderId: expected="
                    + orderId + ", actual=" + returnedOrderId);
        }
    }

    private static void cancelOrder(
            Config config,
            HttpClient http,
            ObjectMapper mapper,
            UUID orderId,
            UUID userId) throws Exception {
        String body = mapper.writeValueAsString(Map.of("orderId", orderId, "userId", userId));
        HttpResponse<String> response = http.send(HttpRequest.newBuilder()
                        .uri(URI.create(config.orderUrl() + "/bid/user-orders/cancel"))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        requireStatus(response, 202, "order cancellation");
        JsonNode result = mapper.readTree(response.body());
        if (!orderId.toString().equals(result.path("orderId").asText())
                || result.path("cancellationId").asText().isBlank()
                || !"CANCELLATION_PENDING".equals(result.path("status").asText())) {
            throw new IllegalStateException("order cancellation response is not traceable: " + response.body());
        }
    }

    private static int visibleAmount(
            Config config,
            HttpClient http,
            ObjectMapper mapper,
            UUID userId,
            UUID orderId) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder()
                        .uri(URI.create(config.matchUrl() + "/v1/order/query/" + userId))
                        .timeout(Duration.ofSeconds(5))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        requireStatus(response, 200, "Match open-order query");
        for (JsonNode order : mapper.readTree(response.body())) {
            if (orderId.toString().equals(order.path("orderId").asText())) {
                return order.path("amount").asInt();
            }
        }
        return -1;
    }

    private static void requireStatus(HttpResponse<String> response, int expected, String operation) {
        if (response.statusCode() != expected) {
            throw new IllegalStateException(operation + " failed: expected=" + expected
                    + ", actual=" + response.statusCode() + ", body=" + response.body());
        }
    }

    private static Connection open(String jdbcUrl, Config config) throws Exception {
        return DriverManager.getConnection(jdbcUrl, config.jdbcUser(), config.jdbcPassword());
    }

    private static WalletBalance walletBalance(Connection connection, UUID userId) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT available_amount, locked_amount, available_currency, locked_currency
                FROM wallet_service.wallets
                WHERE user_id = ?
                """)) {
            statement.setObject(1, userId);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("wallet not found: " + userId);
                }
                return new WalletBalance(rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getInt(4));
            }
        }
    }

    private static boolean walletOrderClaimed(Connection connection, UUID orderId, UUID userId) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT EXISTS (
                    SELECT 1
                    FROM wallet_service.order_submission_idempotency
                    WHERE order_id = ? AND user_id = ?
                )
                """)) {
            statement.setObject(1, orderId);
            statement.setObject(2, userId);
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    private static CancellationApplication cancellationApplication(Connection connection, UUID orderId)
            throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT cancellation_id, cancelled_quantity
                FROM wallet_service.order_cancellation_applications
                WHERE order_id = ?
                """)) {
            statement.setObject(1, orderId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? new CancellationApplication(
                        rs.getObject(1, UUID.class), rs.getInt(2)) : null;
            }
        }
    }

    private static OrderState orderState(Connection connection, UUID orderId) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT remaining_amount, matched_amount, status
                FROM order_service.order_matching_state
                WHERE order_id = ?
                """)) {
            statement.setObject(1, orderId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? new OrderState(rs.getInt(1), rs.getInt(2), rs.getString(3)) : null;
            }
        }
    }

    private static CancellationDecision cancellationDecision(Connection connection, UUID orderId) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT status, cancelled_amount
                FROM match_engine.order_cancellations
                WHERE order_id = ?
                """)) {
            statement.setObject(1, orderId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? new CancellationDecision(rs.getString(1), rs.getInt(2)) : null;
            }
        }
    }

    private static long tradeCount(Connection connection, UUID orderId) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT count(*)
                FROM match_engine.trade_executions
                WHERE buyer_order_id = ? OR seller_order_id = ?
                """)) {
            statement.setObject(1, orderId);
            statement.setObject(2, orderId);
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static String singleTradeId(Connection connection, UUID orderId) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT trade_id
                FROM match_engine.trade_executions
                WHERE buyer_order_id = ? OR seller_order_id = ?
                ORDER BY trade_id
                """)) {
            statement.setObject(1, orderId);
            statement.setObject(2, orderId);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                String tradeId = rs.getString(1);
                if (rs.next()) {
                    throw new IllegalStateException("expected one trade for order " + orderId);
                }
                return tradeId;
            }
        }
    }

    private static boolean hasTradeId(Connection connection, String table, String tradeId) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM " + table + " WHERE trade_id = ?)")) {
            statement.setString(1, tradeId);
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    private static long tradeCount(Connection connection, String table, UUID orderId) throws Exception {
        String sql = "SELECT count(*) FROM " + table + " WHERE buyer_order_id = ? OR seller_order_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, orderId);
            statement.setObject(2, orderId);
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static Set<String> tradeIds(Connection connection, String sql, UUID orderId) throws Exception {
        Set<String> result = new TreeSet<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, orderId);
            statement.setObject(2, orderId);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    result.add(rs.getString(1));
                }
            }
        }
        return result;
    }

    private static Set<String> tradeIdsByExpected(Connection connection, Set<String> expected) throws Exception {
        Set<String> result = new TreeSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT trade_id FROM wallet_service.trade_settlements WHERE trade_id = ANY(?)")) {
            var array = connection.createArrayOf("varchar", expected.toArray(String[]::new));
            statement.setArray(1, array);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    result.add(rs.getString(1));
                }
            } finally {
                array.free();
            }
        }
        return result;
    }

    private static long scalarLong(Connection connection, String sql) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static UUID stableId(String runId, String suffix) {
        return UUID.nameUUIDFromBytes((runId + ":" + suffix).getBytes(StandardCharsets.UTF_8));
    }

    private static void await(String description, int timeoutSeconds, CheckedCondition condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        Exception lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.test()) {
                    return;
                }
                lastFailure = null;
            } catch (Exception e) {
                lastFailure = e;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        throw new IllegalStateException("timed out waiting for " + description, lastFailure);
    }

    @FunctionalInterface
    private interface CheckedCondition {
        boolean test() throws Exception;
    }

    @FunctionalInterface
    private interface CheckedAction {
        void run() throws Exception;
    }

    private record WalletBalance(
            int availableAmount,
            int lockedAmount,
            int availableCurrency,
            int lockedCurrency) {
    }

    private record CancellationApplication(UUID cancellationId, int cancelledQuantity) {
    }

    private record OrderState(int remainingAmount, int matchedAmount, String status) {
    }

    private record CancellationDecision(String status, int cancelledAmount) {

        private boolean terminal() {
            return "CANCELLED".equals(status)
                    || "ALREADY_MATCHED".equals(status)
                    || "NOT_OPEN".equals(status);
        }
    }

    private record PartialScenario(String tradeId, boolean threeServiceTradeIdsEqual) {
    }

    private record RaceSummary(int cancellationWins, int matchingWins, int cleanupCancellations) {
    }

    private record Config(
            String runId,
            String orderUrl,
            String walletUrl,
            String matchUrl,
            String orderJdbcUrl,
            String walletJdbcUrl,
            String matchJdbcUrl,
            String jdbcUser,
            String jdbcPassword,
            String rabbitManagementUrl,
            String rabbitVhost,
            String rabbitUser,
            String rabbitPassword,
            int timeoutSeconds,
            int raceIterations) {

        static Config from(String[] args) {
            Map<String, String> values = new LinkedHashMap<>();
            for (int i = 0; i < args.length; i += 2) {
                if (i + 1 >= args.length || !args[i].startsWith("--")) {
                    throw new IllegalArgumentException("arguments must be --name value pairs");
                }
                values.put(args[i].substring(2), args[i + 1]);
            }
            return new Config(
                    values.getOrDefault("run-id", "CANCELLATION_LIFECYCLE"),
                    values.getOrDefault("order-url", "http://localhost:8080/eap-order"),
                    values.getOrDefault("wallet-url", "http://localhost:8081/eap-wallet"),
                    values.getOrDefault("match-url", "http://localhost:8082/match-engine"),
                    values.getOrDefault("order-jdbc-url", "jdbc:postgresql://localhost:15432/eap_order_db"),
                    values.getOrDefault("wallet-jdbc-url", "jdbc:postgresql://localhost:15433/eap_wallet_db"),
                    values.getOrDefault("match-jdbc-url", "jdbc:postgresql://localhost:15434/eap_match_db"),
                    values.getOrDefault("jdbc-user", "admin"),
                    System.getenv().getOrDefault("EAP_LOADTEST_JDBC_PASSWORD", "admin123"),
                    values.getOrDefault("rabbit-management-url", "http://localhost:15672"),
                    System.getenv().getOrDefault("EAP_LOADTEST_RABBIT_VHOST", "/"),
                    values.getOrDefault("rabbit-user", "admin"),
                    System.getenv().getOrDefault("EAP_LOADTEST_RABBIT_PASSWORD", "admin123"),
                    Integer.parseInt(values.getOrDefault("timeout-seconds", "120")),
                    Integer.parseInt(values.getOrDefault("race-iterations", "10")));
        }
    }
}
