package com.eap.eap_order.loadtest;

import com.eap.common.event.OrderConfirmedEvent;
import com.eap.common.event.OrderSubmittedEvent;
import com.eap.eap_order.EapOrderApplication;
import com.eap.eap_order.application.OrderEventSourcingService;
import com.eap.eap_order.eventstore.OrdersCurrentProjector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import static com.eap.common.constants.RabbitMQConstants.DEAD_LETTER_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.MATCH_ENGINE_ORDER_TRADE_APPLIED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.MATCH_ENGINE_ORDER_CONFIRMED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.MATCH_ENGINE_WALLET_TRADE_SETTLED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_AUCTION_CLEARED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_AUCTION_CREATED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_CREATE_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_CREATED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_FAILED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_MATCHED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_CONFIRMED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_FAILED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_TRADE_EXECUTED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_AUCTION_BID_SUBMITTED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_AUCTION_CLEARED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_ORDER_MATCHED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_ORDER_SUBMITTED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_TRADE_EXECUTED_QUEUE;

public class MatchedE2eLoadGenerator {

    private static final int PRICE = 100;
    private static final int AMOUNT = 1;

    public static void main(String[] args) throws Exception {
        Config config = Config.from(args);

        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(EapOrderApplication.class)
                .profiles("loadtest")
                .web(WebApplicationType.NONE)
                .properties(
                        "spring.rabbitmq.listener.simple.auto-startup=false",
                        "spring.datasource.url=" + config.orderJdbcUrl(),
                        "spring.datasource.username=admin",
                        "spring.datasource.password=admin123",
                        "spring.datasource.driver-class-name=org.postgresql.Driver",
                        "spring.jpa.hibernate.ddl-auto=validate",
                        "spring.liquibase.enabled=true",
                        "spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.xml",
                        "eap.scheduling.enabled=false",
                        "eap.order-event-outbox.batch-size=0",
                        "eap.order-projection.enabled=" + config.projectionPhase(),
                        "eap.order-projection.repair.enabled=false",
                        "eap.matchEngine.base-url=http://localhost:8082/match-engine",
                        "eap.wallet.base-url=http://localhost:8081/eap-wallet",
                        "logging.level.com.eap.eap_order=WARN",
                        "logging.level.org.springframework.amqp=WARN",
                        "logging.level.org.hibernate=WARN")
                .run()) {

            JdbcTemplate orderJdbc = context.getBean(JdbcTemplate.class);
            JdbcTemplate walletJdbc = jdbcTemplate(config.walletJdbcUrl());
            JdbcTemplate matchJdbc = jdbcTemplate(config.matchJdbcUrl());
            OrderEventSourcingService eventSourcingService = context.getBean(OrderEventSourcingService.class);
            OrdersCurrentProjector ordersCurrentProjector = context.getBean(OrdersCurrentProjector.class);
            ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

            CachingConnectionFactory rabbitConnectionFactory = rabbitConnectionFactory(config);
            RabbitTemplate rabbitTemplate = rabbitTemplate(rabbitConnectionFactory, objectMapper);
            RabbitAdmin rabbitAdmin = new RabbitAdmin(rabbitConnectionFactory);
            LettuceConnectionFactory redisConnectionFactory = redisConnectionFactory(config);
            RedisTemplate<String, String> redisTemplate = redisTemplate(redisConnectionFactory);

            try {
                List<Pair> pairs = buildPairs(config);
                switch (config.phase()) {
                    case "seed" -> seed(config, pairs, orderJdbc, walletJdbc, matchJdbc, eventSourcingService, rabbitAdmin, redisTemplate);
                    case "project" -> projectSeededOrders(config, orderJdbc, ordersCurrentProjector);
                    case "run" -> run(config, pairs, orderJdbc, walletJdbc, matchJdbc, rabbitTemplate, rabbitAdmin, redisTemplate);
                    case "all" -> {
                        seed(config, pairs, orderJdbc, walletJdbc, matchJdbc, eventSourcingService, rabbitAdmin, redisTemplate);
                        projectSeededOrders(config, orderJdbc, ordersCurrentProjector);
                        run(config, pairs, orderJdbc, walletJdbc, matchJdbc, rabbitTemplate, rabbitAdmin, redisTemplate);
                    }
                    default -> throw new IllegalArgumentException("--phase must be seed, project, run, or all");
                }
            } finally {
                redisConnectionFactory.destroy();
                rabbitConnectionFactory.destroy();
            }
        }
    }

    private static void seed(
            Config config,
            List<Pair> pairs,
            JdbcTemplate orderJdbc,
            JdbcTemplate walletJdbc,
            JdbcTemplate matchJdbc,
            OrderEventSourcingService eventSourcingService,
            RabbitAdmin rabbitAdmin,
            RedisTemplate<String, String> redisTemplate) {
        if (config.truncate()) {
            truncateOrderTestData(orderJdbc);
            truncateWalletTestData(walletJdbc);
            truncateMatchTestData(matchJdbc);
        }
        purgeQueues(rabbitAdmin);
        cleanupRedis(config, pairs, redisTemplate);

        System.out.printf("seeding %d matched pairs into Order Event Store and Wallet DB, marketId=%s%n",
                config.events(), config.marketId());
        for (Pair pair : pairs) {
            OrderSubmittedEvent buySubmitted = submitted(pair.buyOrderId(), pair.buyerId(), "BUY", pair.buySequence(), config.marketId());
            OrderSubmittedEvent sellSubmitted = submitted(pair.sellOrderId(), pair.sellerId(), "SELL", pair.sellSequence(), config.marketId());
            eventSourcingService.request(buySubmitted);
            eventSourcingService.confirm(confirmed(buySubmitted));
            eventSourcingService.request(sellSubmitted);
            eventSourcingService.confirm(confirmed(sellSubmitted));
        }

        orderJdbc.execute("TRUNCATE TABLE order_service.order_event_outbox RESTART IDENTITY");
        seedWallets(walletJdbc, pairs);
        System.out.println("seed complete; start matchEngine/order/wallet services before --phase run");
    }

    private static void projectSeededOrders(
            Config config,
            JdbcTemplate orderJdbc,
            OrdersCurrentProjector ordersCurrentProjector) {
        System.out.printf("projecting seeded Order Event Store into orders_current, marketId=%s%n", config.marketId());
        ordersCurrentProjector.projectUntilCaughtUpIgnoringEnabled();
        long openRows = count(orderJdbc, """
                SELECT count(*)
                FROM order_service.orders_current
                WHERE market_id = ?
                  AND status = 'OPEN'
                  AND matched_amount = 0
                  AND remaining_amount = original_amount
                """, config.marketId());
        require(openRows == config.events() * 2L,
                "Seed projection should mark buyer and seller orders as OPEN before run phase");
        System.out.printf("projection prewarm complete; openOrders=%d%n", openRows);
    }

    private static void run(
            Config config,
            List<Pair> pairs,
            JdbcTemplate orderJdbc,
            JdbcTemplate walletJdbc,
            JdbcTemplate matchJdbc,
            RabbitTemplate rabbitTemplate,
            RabbitAdmin rabbitAdmin,
            RedisTemplate<String, String> redisTemplate) throws Exception {
        purgeQueues(rabbitAdmin);
        cleanupRedis(config, pairs, redisTemplate);

        System.out.printf("publishing %d resting SELL confirmations directly to %s%n",
                config.events(), MATCH_ENGINE_ORDER_CONFIRMED_QUEUE);
        PublishResult sellPublish = publishToMatchEngine(config, rabbitTemplate, pairs, true);
        waitForRedisSellBook(config, redisTemplate, rabbitAdmin);

        System.out.printf("publishing %d incoming BUY confirmations directly to %s%n",
                config.events(), MATCH_ENGINE_ORDER_CONFIRMED_QUEUE);
        long started = System.nanoTime();
        PublishResult buyPublish = publishBuyToMatchEngine(config, rabbitTemplate, pairs);
        WaitResult waitResult = waitForDownstream(config, orderJdbc, walletJdbc, matchJdbc, rabbitAdmin, started);
        double elapsedSeconds = waitResult.completedTradesReachedSeconds() > 0
                ? waitResult.completedTradesReachedSeconds()
                : (System.nanoTime() - started) / 1_000_000_000.0;

        long remainingSellOrders = zsetSize(redisTemplate, "orderbook:" + config.marketId() + ":sell");
        long remainingBuyOrders = zsetSize(redisTemplate, "orderbook:" + config.marketId() + ":buy");
        printResult(config, sellPublish, buyPublish, waitResult, elapsedSeconds, remainingSellOrders, remainingBuyOrders);

        require(sellPublish.failures() == 0, "SELL publish should have no failures");
        require(buyPublish.failures() == 0, "BUY publish should have no failures");
        require(waitResult.orderMatchedEvents() == config.events() * 2L, "Order should append buyer and seller matched events");
        require(waitResult.orderCurrentMatchedRows() == config.events() * 2L,
                "Order projection should mark buyer and seller orders as MATCHED");
        require(waitResult.tradeExecutions() == config.events(), "MatchEngine should persist one TradeExecuted per match");
        require(waitResult.completedTrades() == config.events(), "Trade completion view should complete every match");
        require(waitResult.walletTradeSettlements() == config.events(), "Wallet should settle every TradeExecuted exactly once");
        require(waitResult.lockedCurrency() == 0, "Wallet buyer locked currency should be released");
        require(waitResult.lockedAmount() == 0, "Wallet seller locked amount should be released");
        require(waitResult.buyerAvailableAmount() == config.events() * (long) AMOUNT, "Wallet buyers should receive energy");
        require(waitResult.sellerAvailableCurrency() == config.events() * (long) PRICE * AMOUNT, "Wallet sellers should receive currency");
        require(remainingSellOrders == 0, "all resting SELL orders should be consumed");
        require(remainingBuyOrders == 0, "incoming BUY orders should not remain in order book");
    }

    private static void seedWallets(JdbcTemplate jdbcTemplate, List<Pair> pairs) {
        List<Object[]> batchArgs = new ArrayList<>(pairs.size() * 2);
        for (Pair pair : pairs) {
            batchArgs.add(new Object[] { pair.buyerId(), 0, 0, 0, PRICE * AMOUNT });
            batchArgs.add(new Object[] { pair.sellerId(), 0, AMOUNT, 0, 0 });
        }
        jdbcTemplate.batchUpdate("""
                INSERT INTO wallet_service.wallets (
                    user_id,
                    available_amount,
                    locked_amount,
                    available_currency,
                    locked_currency,
                    update_time,
                    version
                )
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 0)
                """, batchArgs);
    }

    private static PublishResult publishToMatchEngine(
            Config config,
            RabbitTemplate rabbitTemplate,
            List<Pair> pairs,
            boolean sell) throws InterruptedException {
        return publishToMatchEngine(config, rabbitTemplate, pairs, sell, 0);
    }

    private static PublishResult publishBuyToMatchEngine(
            Config config,
            RabbitTemplate rabbitTemplate,
            List<Pair> pairs) throws InterruptedException {
        return publishToMatchEngine(config, rabbitTemplate, pairs, false, config.targetTps());
    }

    private static PublishResult publishToMatchEngine(
            Config config,
            RabbitTemplate rabbitTemplate,
            List<Pair> pairs,
            boolean sell,
            int targetTps) throws InterruptedException {
        AtomicInteger published = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(pairs.size());
        ExecutorService executor = Executors.newFixedThreadPool(config.publishers());
        Semaphore inFlight = new Semaphore(config.publishers() * 2);

        long started = System.nanoTime();
        long intervalNanos = targetTps > 0 ? Math.max(1L, 1_000_000_000L / targetTps) : 0L;
        long scheduledIndex = 0;
        for (Pair pair : pairs) {
            if (intervalNanos > 0) {
                sleepUntil(started + intervalNanos * scheduledIndex);
                scheduledIndex++;
            }
            inFlight.acquire();
            executor.execute(() -> {
                try {
                    OrderConfirmedEvent event = sell ? pair.sellConfirmed(config.marketId()) : pair.buyConfirmed(config.marketId());
                    rabbitTemplate.convertAndSend("", MATCH_ENGINE_ORDER_CONFIRMED_QUEUE, event);
                    published.incrementAndGet();
                } catch (Exception e) {
                    failures.incrementAndGet();
                    if (failures.get() <= 10) {
                        System.err.printf("publish failed: sell=%s, error=%s%n", sell, e.getMessage());
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
        double elapsedSeconds = (System.nanoTime() - started) / 1_000_000_000.0;
        return new PublishResult(published.get(), failures.get(), elapsedSeconds, targetTps);
    }

    private static void sleepUntil(long targetNanos) throws InterruptedException {
        while (true) {
            long remainingNanos = targetNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                return;
            }
            TimeUnit.NANOSECONDS.sleep(Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(1)));
        }
    }

    private static void waitForRedisSellBook(
            Config config,
            RedisTemplate<String, String> redisTemplate,
            RabbitAdmin rabbitAdmin) throws InterruptedException {
        String key = "orderbook:" + config.marketId() + ":sell";
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.timeoutSeconds());
        long latestSize = 0;
        while (System.nanoTime() < deadline) {
            latestSize = zsetSize(redisTemplate, key);
            if (latestSize == config.events()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        printSellBookTimeoutDiagnostics(config, redisTemplate, rabbitAdmin, key, latestSize);
        throw new IllegalStateException("Timed out waiting for resting SELL orders in Redis order book");
    }

    private static void printSellBookTimeoutDiagnostics(
            Config config,
            RedisTemplate<String, String> redisTemplate,
            RabbitAdmin rabbitAdmin,
            String sellBookKey,
            long latestSellBookSize) {
        String buyBookKey = "orderbook:" + config.marketId() + ":buy";
        System.err.println("[DIAG] timed out waiting for resting SELL orders");
        System.err.printf("[DIAG] marketId=%s expectedSellBookSize=%d actualSellBookSize=%d buyBookSize=%d%n",
                config.marketId(), config.events(), latestSellBookSize, zsetSize(redisTemplate, buyBookKey));
        System.err.printf("[DIAG] redis.sellBookKey=%s redis.buyBookKey=%s%n", sellBookKey, buyBookKey);
        System.err.printf("[DIAG] queue.%s.ready=%d%n",
                MATCH_ENGINE_ORDER_CONFIRMED_QUEUE, queueReady(rabbitAdmin, MATCH_ENGINE_ORDER_CONFIRMED_QUEUE));
        System.err.printf("[DIAG] queue.%s.ready=%d%n",
                ORDER_TRADE_EXECUTED_QUEUE, queueReady(rabbitAdmin, ORDER_TRADE_EXECUTED_QUEUE));
        System.err.printf("[DIAG] queue.%s.ready=%d%n",
                WALLET_TRADE_EXECUTED_QUEUE, queueReady(rabbitAdmin, WALLET_TRADE_EXECUTED_QUEUE));
        System.err.printf("[DIAG] queue.%s.ready=%d%n",
                MATCH_ENGINE_ORDER_TRADE_APPLIED_QUEUE, queueReady(rabbitAdmin, MATCH_ENGINE_ORDER_TRADE_APPLIED_QUEUE));
        System.err.printf("[DIAG] queue.%s.ready=%d%n",
                MATCH_ENGINE_WALLET_TRADE_SETTLED_QUEUE, queueReady(rabbitAdmin, MATCH_ENGINE_WALLET_TRADE_SETTLED_QUEUE));
    }

    private static WaitResult waitForDownstream(
            Config config,
            JdbcTemplate orderJdbc,
            JdbcTemplate walletJdbc,
            JdbcTemplate matchJdbc,
            RabbitAdmin rabbitAdmin,
            long startedNanos)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.timeoutSeconds());
        WaitResult latest = null;
        double tradeExecutionsReachedSeconds = -1;
        double orderMatchedReachedSeconds = -1;
        double walletSettlementsReachedSeconds = -1;
        double completedTradesReachedSeconds = -1;
        long maxMatchEngineQueueReady = 0;
        long maxOrderTradeExecutedQueueReady = 0;
        long maxWalletTradeExecutedQueueReady = 0;
        long maxOrderTradeAppliedQueueReady = 0;
        long maxWalletTradeSettledQueueReady = 0;
        int consecutiveDrainedSamples = 0;
        double queueDrainedSeconds = -1;
        do {
            latest = queueSnapshot(rabbitAdmin);
            double elapsedSeconds = (System.nanoTime() - startedNanos) / 1_000_000_000.0;
            maxMatchEngineQueueReady = Math.max(maxMatchEngineQueueReady, latest.matchEngineQueueReady());
            maxOrderTradeExecutedQueueReady = Math.max(maxOrderTradeExecutedQueueReady, latest.orderTradeExecutedQueueReady());
            maxWalletTradeExecutedQueueReady = Math.max(maxWalletTradeExecutedQueueReady, latest.walletTradeExecutedQueueReady());
            maxOrderTradeAppliedQueueReady = Math.max(maxOrderTradeAppliedQueueReady, latest.orderTradeAppliedQueueReady());
            maxWalletTradeSettledQueueReady = Math.max(maxWalletTradeSettledQueueReady, latest.walletTradeSettledQueueReady());
            if (queuesDrained(latest)) {
                consecutiveDrainedSamples++;
            } else {
                consecutiveDrainedSamples = 0;
            }
            if (consecutiveDrainedSamples >= 3) {
                queueDrainedSeconds = elapsedSeconds;
                tradeExecutionsReachedSeconds = queueDrainedSeconds;
                orderMatchedReachedSeconds = queueDrainedSeconds;
                walletSettlementsReachedSeconds = queueDrainedSeconds;
                completedTradesReachedSeconds = queueDrainedSeconds;
                break;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        } while (System.nanoTime() < deadline);

        if (queueDrainedSeconds >= 0) {
            do {
                WaitResult verified = snapshot(orderJdbc, walletJdbc, matchJdbc, rabbitAdmin);
                if (invariantsSatisfied(config, verified)) {
                    return verified.withDiagnostics(
                            tradeExecutionsReachedSeconds,
                            orderMatchedReachedSeconds,
                            walletSettlementsReachedSeconds,
                            completedTradesReachedSeconds,
                            maxMatchEngineQueueReady,
                            maxOrderTradeExecutedQueueReady,
                            maxWalletTradeExecutedQueueReady,
                            maxOrderTradeAppliedQueueReady,
                            maxWalletTradeSettledQueueReady);
                }
                TimeUnit.MILLISECONDS.sleep(250);
            } while (System.nanoTime() < deadline);
        }

        WaitResult finalSnapshot = snapshot(orderJdbc, walletJdbc, matchJdbc, rabbitAdmin);
        return finalSnapshot.withDiagnostics(
                tradeExecutionsReachedSeconds,
                orderMatchedReachedSeconds,
                walletSettlementsReachedSeconds,
                completedTradesReachedSeconds,
                maxMatchEngineQueueReady,
                maxOrderTradeExecutedQueueReady,
                maxWalletTradeExecutedQueueReady,
                maxOrderTradeAppliedQueueReady,
                maxWalletTradeSettledQueueReady);
    }

    private static boolean queuesDrained(WaitResult latest) {
        return latest.matchEngineQueueReady() == 0
                && latest.orderMatchedQueueReady() == 0
                && latest.walletMatchedQueueReady() == 0
                && latest.orderTradeExecutedQueueReady() == 0
                && latest.walletTradeExecutedQueueReady() == 0
                && latest.orderTradeAppliedQueueReady() == 0
                && latest.walletTradeSettledQueueReady() == 0;
    }

    private static boolean invariantsSatisfied(Config config, WaitResult verified) {
        return verified.orderMatchedEvents() == config.events() * 2L
                && verified.tradeExecutions() == config.events()
                && verified.completedTrades() == config.events()
                && verified.walletTradeSettlements() == config.events()
                && verified.orderCurrentMatchedRows() == config.events() * 2L
                && verified.lockedCurrency() == 0
                && verified.lockedAmount() == 0
                && verified.buyerAvailableAmount() == config.events() * (long) AMOUNT
                && verified.sellerAvailableCurrency() == config.events() * (long) PRICE * AMOUNT;
    }

    private static WaitResult queueSnapshot(RabbitAdmin rabbitAdmin) {
        return new WaitResult(
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                queueReady(rabbitAdmin, MATCH_ENGINE_ORDER_CONFIRMED_QUEUE),
                queueReady(rabbitAdmin, ORDER_ORDER_MATCHED_QUEUE),
                queueReady(rabbitAdmin, WALLET_ORDER_MATCHED_QUEUE),
                queueReady(rabbitAdmin, ORDER_TRADE_EXECUTED_QUEUE),
                queueReady(rabbitAdmin, WALLET_TRADE_EXECUTED_QUEUE),
                queueReady(rabbitAdmin, MATCH_ENGINE_ORDER_TRADE_APPLIED_QUEUE),
                queueReady(rabbitAdmin, MATCH_ENGINE_WALLET_TRADE_SETTLED_QUEUE),
                -1,
                -1,
                -1,
                -1,
                0,
                0,
                0,
                0,
                0);
    }

    private static WaitResult snapshot(
            JdbcTemplate orderJdbc,
            JdbcTemplate walletJdbc,
            JdbcTemplate matchJdbc,
            RabbitAdmin rabbitAdmin) {
        return new WaitResult(
                count(orderJdbc, "SELECT count(*) FROM order_service.order_event_store WHERE event_type = 'OrderMatchedV1'"),
                count(orderJdbc, """
                        SELECT count(*)
                        FROM order_service.orders_current
                        WHERE status = 'MATCHED'
                          AND remaining_amount = 0
                          AND matched_amount = original_amount
                        """),
                count(orderJdbc, "SELECT count(*) FROM order_service.match_history"),
                count(matchJdbc, "SELECT count(*) FROM match_engine.trade_executions"),
                count(matchJdbc, "SELECT count(*) FROM match_engine.trade_completion_view WHERE completed_at IS NOT NULL"),
                count(walletJdbc, "SELECT count(*) FROM wallet_service.trade_settlements"),
                count(walletJdbc, "SELECT COALESCE(sum(locked_currency), 0) FROM wallet_service.wallets"),
                count(walletJdbc, "SELECT COALESCE(sum(locked_amount), 0) FROM wallet_service.wallets"),
                count(walletJdbc, "SELECT COALESCE(sum(available_amount), 0) FROM wallet_service.wallets"),
                count(walletJdbc, "SELECT COALESCE(sum(available_currency), 0) FROM wallet_service.wallets"),
                queueReady(rabbitAdmin, MATCH_ENGINE_ORDER_CONFIRMED_QUEUE),
                queueReady(rabbitAdmin, ORDER_ORDER_MATCHED_QUEUE),
                queueReady(rabbitAdmin, WALLET_ORDER_MATCHED_QUEUE),
                queueReady(rabbitAdmin, ORDER_TRADE_EXECUTED_QUEUE),
                queueReady(rabbitAdmin, WALLET_TRADE_EXECUTED_QUEUE),
                queueReady(rabbitAdmin, MATCH_ENGINE_ORDER_TRADE_APPLIED_QUEUE),
                queueReady(rabbitAdmin, MATCH_ENGINE_WALLET_TRADE_SETTLED_QUEUE),
                -1,
                -1,
                -1,
                -1,
                0,
                0,
                0,
                0,
                0);
    }

    private static void truncateOrderTestData(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute("""
                TRUNCATE TABLE
                    order_service.match_history,
                    order_service.order_execution_links,
                    order_service.order_event_outbox,
                    order_service.orders_current,
                    order_service.projection_checkpoints,
                    order_service.order_event_store,
                    order_service.order_stream_heads
                RESTART IDENTITY CASCADE
                """);
    }

    private static void truncateWalletTestData(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute("""
                TRUNCATE TABLE
                    wallet_service.trade_settlements,
                    wallet_service.outbox,
                    wallet_service.order_submission_idempotency,
                    wallet_service.settlement_idempotency,
                    wallet_service.wallets
                RESTART IDENTITY CASCADE
                """);
    }

    private static void truncateMatchTestData(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute("""
                TRUNCATE TABLE
                    match_engine.trade_completion_view,
                    match_engine.trade_outbox,
                    match_engine.trade_executions
                RESTART IDENTITY CASCADE
                """);
    }

    private static void purgeQueues(RabbitAdmin rabbitAdmin) {
        purgeIfPresent(rabbitAdmin, WALLET_ORDER_SUBMITTED_QUEUE);
        purgeIfPresent(rabbitAdmin, MATCH_ENGINE_ORDER_CONFIRMED_QUEUE);
        purgeIfPresent(rabbitAdmin, ORDER_ORDER_CONFIRMED_QUEUE);
        purgeIfPresent(rabbitAdmin, ORDER_ORDER_FAILED_QUEUE);
        purgeIfPresent(rabbitAdmin, ORDER_ORDER_MATCHED_QUEUE);
        purgeIfPresent(rabbitAdmin, WALLET_ORDER_MATCHED_QUEUE);
        purgeIfPresent(rabbitAdmin, ORDER_TRADE_EXECUTED_QUEUE);
        purgeIfPresent(rabbitAdmin, WALLET_TRADE_EXECUTED_QUEUE);
        purgeIfPresent(rabbitAdmin, MATCH_ENGINE_ORDER_TRADE_APPLIED_QUEUE);
        purgeIfPresent(rabbitAdmin, MATCH_ENGINE_WALLET_TRADE_SETTLED_QUEUE);
        purgeIfPresent(rabbitAdmin, WALLET_AUCTION_BID_SUBMITTED_QUEUE);
        purgeIfPresent(rabbitAdmin, WALLET_AUCTION_CLEARED_QUEUE);
        purgeIfPresent(rabbitAdmin, ORDER_AUCTION_CLEARED_QUEUE);
        purgeIfPresent(rabbitAdmin, ORDER_AUCTION_CREATED_QUEUE);
        purgeIfPresent(rabbitAdmin, ORDER_CREATE_QUEUE);
        purgeIfPresent(rabbitAdmin, ORDER_CREATED_QUEUE);
        purgeIfPresent(rabbitAdmin, ORDER_FAILED_QUEUE);
        purgeIfPresent(rabbitAdmin, DEAD_LETTER_QUEUE);
    }

    private static void purgeIfPresent(RabbitAdmin rabbitAdmin, String queueName) {
        try {
            if (rabbitAdmin.getQueueProperties(queueName) != null) {
                rabbitAdmin.purgeQueue(queueName, false);
            }
        } catch (Exception ignored) {
        }
    }

    private static void cleanupRedis(Config config, List<Pair> pairs, RedisTemplate<String, String> redisTemplate) {
        List<String> keys = new ArrayList<>(pairs.size() * 4 + 2);
        keys.add("orderbook:" + config.marketId() + ":buy");
        keys.add("orderbook:" + config.marketId() + ":sell");
        for (Pair pair : pairs) {
            keys.add("order:" + pair.buyOrderId());
            keys.add("order:" + pair.sellOrderId());
            keys.add("user:" + pair.buyerId() + ":orders");
            keys.add("user:" + pair.sellerId() + ":orders");
        }
        redisTemplate.delete(keys);
    }

    private static OrderSubmittedEvent submitted(UUID orderId, UUID userId, String side, long sequence, String marketId) {
        return OrderSubmittedEvent.builder()
                .orderId(orderId)
                .userId(userId)
                .marketId(marketId)
                .marketSequence(sequence)
                .price(PRICE)
                .amount(AMOUNT)
                .orderType(side)
                .createdAt(LocalDateTime.now())
                .build();
    }

    private static OrderConfirmedEvent confirmed(OrderSubmittedEvent submitted) {
        return OrderConfirmedEvent.builder()
                .orderId(submitted.getOrderId())
                .userId(submitted.getUserId())
                .marketId(submitted.getMarketId())
                .marketSequence(submitted.getMarketSequence())
                .price(submitted.getPrice())
                .amount(submitted.getAmount())
                .orderType(submitted.getOrderType())
                .createdAt(submitted.getCreatedAt())
                .build();
    }

    private static List<Pair> buildPairs(Config config) {
        List<Pair> pairs = new ArrayList<>(config.events());
        for (int i = 0; i < config.events(); i++) {
            int index = i + 1;
            pairs.add(new Pair(
                    index,
                    deterministicUuid(config.marketId(), "buyer", index),
                    deterministicUuid(config.marketId(), "seller", index),
                    deterministicUuid(config.marketId(), "buyOrder", index),
                    deterministicUuid(config.marketId(), "sellOrder", index),
                    (long) index * 2L - 1L,
                    (long) index * 2L));
        }
        return pairs;
    }

    private static UUID deterministicUuid(String marketId, String type, int index) {
        return UUID.nameUUIDFromBytes((marketId + ":" + type + ":" + index).getBytes(StandardCharsets.UTF_8));
    }

    private static CachingConnectionFactory rabbitConnectionFactory(Config config) {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(config.rabbitHost(), config.rabbitPort());
        connectionFactory.setUsername(config.rabbitUser());
        connectionFactory.setPassword(config.rabbitPassword());
        return connectionFactory;
    }

    private static RabbitTemplate rabbitTemplate(CachingConnectionFactory connectionFactory, ObjectMapper objectMapper) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter(objectMapper));
        rabbitTemplate.setMandatory(true);
        return rabbitTemplate;
    }

    private static LettuceConnectionFactory redisConnectionFactory(Config config) {
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(config.redisHost(), config.redisPort());
        connectionFactory.afterPropertiesSet();
        return connectionFactory;
    }

    private static RedisTemplate<String, String> redisTemplate(LettuceConnectionFactory connectionFactory) {
        RedisTemplate<String, String> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(connectionFactory);
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashValueSerializer(new StringRedisSerializer());
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }

    private static long count(JdbcTemplate jdbcTemplate, String sql) {
        Long value = jdbcTemplate.queryForObject(sql, Long.class);
        return value == null ? 0L : value;
    }

    private static long count(JdbcTemplate jdbcTemplate, String sql, Object... args) {
        Long value = jdbcTemplate.queryForObject(sql, Long.class, args);
        return value == null ? 0L : value;
    }

    private static long zsetSize(RedisTemplate<String, String> redisTemplate, String key) {
        Long size = redisTemplate.opsForZSet().size(key);
        return size == null ? 0L : size;
    }

    private static long queueReady(RabbitAdmin rabbitAdmin, String queueName) {
        try {
            Properties properties = rabbitAdmin.getQueueProperties(queueName);
            if (properties == null) {
                return 0;
            }
            Object count = properties.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
            return count instanceof Number number ? number.longValue() : 0;
        } catch (Exception ignored) {
            return 0;
        }
    }

    private static JdbcTemplate jdbcTemplate(String jdbcUrl) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername("admin");
        dataSource.setPassword("admin123");
        return new JdbcTemplate(dataSource);
    }

    private static void printResult(
            Config config,
            PublishResult sellPublish,
            PublishResult buyPublish,
            WaitResult waitResult,
            double elapsedSeconds,
            long remainingSellOrders,
            long remainingBuyOrders) {
        System.out.println("{");
        System.out.printf("  \"mode\": \"matchedE2e\",%n");
        System.out.printf("  \"marketId\": \"%s\",%n", config.marketId());
        System.out.printf("  \"matches\": %d,%n", config.events());
        System.out.printf("  \"publishers\": %d,%n", config.publishers());
        System.out.printf("  \"targetTps\": %d,%n", config.targetTps());
        System.out.printf("  \"durationSeconds\": %d,%n", config.durationSeconds());
        System.out.printf("  \"expectedBuyPublishSeconds\": %.2f,%n", config.expectedBuyPublishSeconds());
        System.out.printf("  \"sellPublished\": %d,%n", sellPublish.published());
        System.out.printf("  \"sellPublishFailures\": %d,%n", sellPublish.failures());
        System.out.printf("  \"sellPublishSeconds\": %.2f,%n", sellPublish.elapsedSeconds());
        System.out.printf("  \"buyPublished\": %d,%n", buyPublish.published());
        System.out.printf("  \"buyPublishFailures\": %d,%n", buyPublish.failures());
        System.out.printf("  \"buyPublishSeconds\": %.2f,%n", buyPublish.elapsedSeconds());
        System.out.printf("  \"actualBuyPublishTps\": %.2f,%n", buyPublish.published() / Math.max(buyPublish.elapsedSeconds(), 0.001));
        System.out.printf("  \"drainSecondsAfterBuyPublish\": %.2f,%n", Math.max(0.0, elapsedSeconds - buyPublish.elapsedSeconds()));
        System.out.printf("  \"elapsedSeconds\": %.2f,%n", elapsedSeconds);
        System.out.printf("  \"matchedE2eTps\": %.2f,%n", config.events() / Math.max(elapsedSeconds, 0.001));
        System.out.printf("  \"tradeExecutionsReachedSeconds\": %.2f,%n", waitResult.tradeExecutionsReachedSeconds());
        System.out.printf("  \"orderMatchedReachedSeconds\": %.2f,%n", waitResult.orderMatchedReachedSeconds());
        System.out.printf("  \"walletSettlementsReachedSeconds\": %.2f,%n", waitResult.walletSettlementsReachedSeconds());
        System.out.printf("  \"completedTradesReachedSeconds\": %.2f,%n", waitResult.completedTradesReachedSeconds());
        System.out.printf("  \"orderMatchedEvents\": %d,%n", waitResult.orderMatchedEvents());
        System.out.printf("  \"orderCurrentMatchedRows\": %d,%n", waitResult.orderCurrentMatchedRows());
        System.out.printf("  \"matchHistoryRows\": %d,%n", waitResult.matchHistoryRows());
        System.out.printf("  \"tradeExecutions\": %d,%n", waitResult.tradeExecutions());
        System.out.printf("  \"completedTrades\": %d,%n", waitResult.completedTrades());
        System.out.printf("  \"walletTradeSettlements\": %d,%n", waitResult.walletTradeSettlements());
        System.out.printf("  \"lockedCurrency\": %d,%n", waitResult.lockedCurrency());
        System.out.printf("  \"lockedAmount\": %d,%n", waitResult.lockedAmount());
        System.out.printf("  \"buyerAvailableAmount\": %d,%n", waitResult.buyerAvailableAmount());
        System.out.printf("  \"sellerAvailableCurrency\": %d,%n", waitResult.sellerAvailableCurrency());
        System.out.printf("  \"matchEngineQueueReady\": %d,%n", waitResult.matchEngineQueueReady());
        System.out.printf("  \"orderMatchedQueueReady\": %d,%n", waitResult.orderMatchedQueueReady());
        System.out.printf("  \"walletMatchedQueueReady\": %d,%n", waitResult.walletMatchedQueueReady());
        System.out.printf("  \"orderTradeExecutedQueueReady\": %d,%n", waitResult.orderTradeExecutedQueueReady());
        System.out.printf("  \"walletTradeExecutedQueueReady\": %d,%n", waitResult.walletTradeExecutedQueueReady());
        System.out.printf("  \"orderTradeAppliedQueueReady\": %d,%n", waitResult.orderTradeAppliedQueueReady());
        System.out.printf("  \"walletTradeSettledQueueReady\": %d,%n", waitResult.walletTradeSettledQueueReady());
        System.out.printf("  \"maxMatchEngineQueueReady\": %d,%n", waitResult.maxMatchEngineQueueReady());
        System.out.printf("  \"maxOrderTradeExecutedQueueReady\": %d,%n", waitResult.maxOrderTradeExecutedQueueReady());
        System.out.printf("  \"maxWalletTradeExecutedQueueReady\": %d,%n", waitResult.maxWalletTradeExecutedQueueReady());
        System.out.printf("  \"maxOrderTradeAppliedQueueReady\": %d,%n", waitResult.maxOrderTradeAppliedQueueReady());
        System.out.printf("  \"maxWalletTradeSettledQueueReady\": %d,%n", waitResult.maxWalletTradeSettledQueueReady());
        System.out.printf("  \"remainingSellOrders\": %d,%n", remainingSellOrders);
        System.out.printf("  \"remainingBuyOrders\": %d%n", remainingBuyOrders);
        System.out.println("}");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private record Pair(
            int matchId,
            UUID buyerId,
            UUID sellerId,
            UUID buyOrderId,
            UUID sellOrderId,
            long buySequence,
            long sellSequence) {

        OrderConfirmedEvent buyConfirmed(String marketId) {
            return confirmed(submitted(buyOrderId, buyerId, "BUY", buySequence, marketId));
        }

        OrderConfirmedEvent sellConfirmed(String marketId) {
            return confirmed(submitted(sellOrderId, sellerId, "SELL", sellSequence, marketId));
        }
    }

    private record PublishResult(int published, int failures, double elapsedSeconds, int targetTps) {
    }

    private record WaitResult(
            long orderMatchedEvents,
            long orderCurrentMatchedRows,
            long matchHistoryRows,
            long tradeExecutions,
            long completedTrades,
            long walletTradeSettlements,
            long lockedCurrency,
            long lockedAmount,
            long buyerAvailableAmount,
            long sellerAvailableCurrency,
            long matchEngineQueueReady,
            long orderMatchedQueueReady,
            long walletMatchedQueueReady,
            long orderTradeExecutedQueueReady,
            long walletTradeExecutedQueueReady,
            long orderTradeAppliedQueueReady,
            long walletTradeSettledQueueReady,
            double tradeExecutionsReachedSeconds,
            double orderMatchedReachedSeconds,
            double walletSettlementsReachedSeconds,
            double completedTradesReachedSeconds,
            long maxMatchEngineQueueReady,
            long maxOrderTradeExecutedQueueReady,
            long maxWalletTradeExecutedQueueReady,
            long maxOrderTradeAppliedQueueReady,
            long maxWalletTradeSettledQueueReady) {
        WaitResult withDiagnostics(
                double tradeExecutionsReachedSeconds,
                double orderMatchedReachedSeconds,
                double walletSettlementsReachedSeconds,
                double completedTradesReachedSeconds,
                long maxMatchEngineQueueReady,
                long maxOrderTradeExecutedQueueReady,
                long maxWalletTradeExecutedQueueReady,
                long maxOrderTradeAppliedQueueReady,
                long maxWalletTradeSettledQueueReady) {
            return new WaitResult(
                    orderMatchedEvents,
                    orderCurrentMatchedRows,
                    matchHistoryRows,
                    tradeExecutions,
                    completedTrades,
                    walletTradeSettlements,
                    lockedCurrency,
                    lockedAmount,
                    buyerAvailableAmount,
                    sellerAvailableCurrency,
                    matchEngineQueueReady,
                    orderMatchedQueueReady,
                    walletMatchedQueueReady,
                    orderTradeExecutedQueueReady,
                    walletTradeExecutedQueueReady,
                    orderTradeAppliedQueueReady,
                    walletTradeSettledQueueReady,
                    tradeExecutionsReachedSeconds,
                    orderMatchedReachedSeconds,
                    walletSettlementsReachedSeconds,
                    completedTradesReachedSeconds,
                    maxMatchEngineQueueReady,
                    maxOrderTradeExecutedQueueReady,
                    maxWalletTradeExecutedQueueReady,
                    maxOrderTradeAppliedQueueReady,
                    maxWalletTradeSettledQueueReady);
        }
    }

    private record Config(
            String phase,
            String marketId,
            int events,
            int publishers,
            int timeoutSeconds,
            int targetTps,
            int durationSeconds,
            boolean truncate,
            String redisHost,
            int redisPort,
            String rabbitHost,
            int rabbitPort,
            String rabbitUser,
            String rabbitPassword,
            String orderJdbcUrl,
            String walletJdbcUrl,
            String matchJdbcUrl) {

        private boolean projectionPhase() {
            return "project".equals(phase);
        }

        private double expectedBuyPublishSeconds() {
            return targetTps > 0 ? events / (double) targetTps : 0.0;
        }

        private static Config from(String[] args) {
            String marketId = stringArg(args, "--market-id", "MATCHED_E2E_LOAD");
            int targetTps = intArg(args, "--target-tps", 0);
            int durationSeconds = intArg(args, "--duration-seconds", 0);
            int defaultEvents = targetTps > 0 && durationSeconds > 0
                    ? targetTps * durationSeconds
                    : 1_000;
            return new Config(
                    stringArg(args, "--phase", "run"),
                    marketId,
                    intArg(args, "--events", defaultEvents),
                    intArg(args, "--publishers", 16),
                    intArg(args, "--timeout-seconds", 60),
                    targetTps,
                    durationSeconds,
                    booleanArg(args, "--truncate", true),
                    stringArg(args, "--redis-host", "localhost"),
                    intArg(args, "--redis-port", 6379),
                    stringArg(args, "--rabbit-host", "localhost"),
                    intArg(args, "--rabbit-port", 5672),
                    stringArg(args, "--rabbit-user", "admin"),
                    stringArg(args, "--rabbit-pass", "admin123"),
                    stringArg(args, "--order-jdbc-url", "jdbc:postgresql://localhost:15432/eap_order_db"),
                    stringArg(args, "--wallet-jdbc-url", "jdbc:postgresql://localhost:15433/eap_wallet_db"),
                    stringArg(args, "--match-jdbc-url", "jdbc:postgresql://localhost:15434/eap_match_db"));
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
