package com.eap.eap_order.loadtest;

import com.eap.common.event.OrderConfirmedEvent;
import com.eap.common.event.OrderSubmittedEvent;
import com.eap.eap_order.EapOrderApplication;
import com.eap.eap_order.application.OrderEventSourcingService;
import com.eap.eap_order.domain.ordersourcing.OrderAssetReservationConfirmedV1;
import com.eap.eap_order.domain.ordersourcing.OrderSubmissionRequestedV1;
import com.eap.eap_order.eventstore.OrdersCurrentProjector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import static com.eap.common.constants.RabbitMQConstants.DEAD_LETTER_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.MATCH_ENGINE_ORDER_TRADE_APPLIED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.MATCH_ENGINE_ORDER_CONFIRMED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.MATCH_ENGINE_WALLET_TRADE_SETTLED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_AUCTION_CLEARED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_AUCTION_CREATED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_CONFIRMED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_FAILED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_TRADE_EXECUTED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_AUCTION_BID_SUBMITTED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_AUCTION_CLEARED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_ORDER_SUBMITTED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_TRADE_EXECUTED_QUEUE;

public class MatchedE2eLoadGenerator {

    private static final int PRICE = 100;
    private static final int AMOUNT = 1;
    private static final String AGGREGATE_TYPE = "Order";
    private static final String GENESIS_HASH = "0".repeat(64);
    private static final int SEED_BATCH_PAIRS = 1_000;
    private static final ObjectMapper METRICS_OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectMapper CANONICAL_OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final QueueDrainDiagnostics EMPTY_QUEUE_DRAIN_DIAGNOSTICS =
            new QueueDrainDiagnostics(List.of(), "none", "none", -1, 0, 0);

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

        System.out.printf("seeding %d matched pairs into Order Event Store and Wallet DB, marketId=%s, seedMode=%s%n",
                config.events(), config.marketId(), config.seedMode());
        if ("service".equalsIgnoreCase(config.seedMode())) {
            seedOrdersThroughService(config, pairs, eventSourcingService);
        } else if ("bulk".equalsIgnoreCase(config.seedMode())) {
            seedOrdersBulk(config, pairs, orderJdbc);
        } else {
            throw new IllegalArgumentException("--seed-mode must be bulk or service");
        }

        orderJdbc.execute("TRUNCATE TABLE order_service.order_event_outbox RESTART IDENTITY");
        seedWallets(walletJdbc, pairs);
        System.out.println("seed complete; start matchEngine/order/wallet services before --phase run");
    }

    private static void seedOrdersThroughService(
            Config config,
            List<Pair> pairs,
            OrderEventSourcingService eventSourcingService) {
        for (Pair pair : pairs) {
            OrderSubmittedEvent buySubmitted = submitted(pair.buyOrderId(), pair.buyerId(), "BUY", pair.buySequence(), config.marketId());
            OrderSubmittedEvent sellSubmitted = submitted(pair.sellOrderId(), pair.sellerId(), "SELL", pair.sellSequence(), config.marketId());
            eventSourcingService.request(buySubmitted);
            eventSourcingService.confirm(confirmed(buySubmitted));
            eventSourcingService.request(sellSubmitted);
            eventSourcingService.confirm(confirmed(sellSubmitted));
        }
    }

    private static void seedOrdersBulk(Config config, List<Pair> pairs, JdbcTemplate orderJdbc) {
        for (int start = 0; start < pairs.size(); start += SEED_BATCH_PAIRS) {
            int end = Math.min(start + SEED_BATCH_PAIRS, pairs.size());
            List<Object[]> headRows = new ArrayList<>((end - start) * 2);
            List<Object[]> eventRows = new ArrayList<>((end - start) * 4);
            List<Object[]> matchingStateRows = new ArrayList<>((end - start) * 2);
            for (int i = start; i < end; i++) {
                Pair pair = pairs.get(i);
                addSeededOrderRows(
                        config.marketId(),
                        pair.buyOrderId(),
                        pair.buyerId(),
                        "BUY",
                        pair.buySequence(),
                        headRows,
                        eventRows,
                        matchingStateRows);
                addSeededOrderRows(
                        config.marketId(),
                        pair.sellOrderId(),
                        pair.sellerId(),
                        "SELL",
                        pair.sellSequence(),
                        headRows,
                        eventRows,
                        matchingStateRows);
            }
            orderJdbc.batchUpdate("""
                    INSERT INTO order_service.order_stream_heads
                        (aggregate_id, current_version, last_event_id, last_hash,
                         updated_at, user_id, remaining_amount, status)
                    VALUES (?, 2, ?, ?, CURRENT_TIMESTAMP, ?, ?, 'OPEN')
                    """, headRows);
            orderJdbc.batchUpdate("""
                    INSERT INTO order_service.order_event_store
                        (event_id, aggregate_id, aggregate_type, aggregate_version,
                         event_type, payload_canonical, metadata_canonical,
                         schema_version, occurred_at, prev_hash, hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                    """, eventRows);
            orderJdbc.batchUpdate("""
                    INSERT INTO order_service.order_matching_state
                        (order_id, user_id, remaining_amount, matched_amount, status, updated_at)
                    VALUES (?, ?, ?, 0, 'OPEN', CURRENT_TIMESTAMP)
                    """, matchingStateRows);
        }
    }

    private static void addSeededOrderRows(
            String marketId,
            UUID orderId,
            UUID userId,
            String side,
            long marketSequence,
            List<Object[]> headRows,
            List<Object[]> eventRows,
            List<Object[]> matchingStateRows) {
        LocalDateTime occurredAt = LocalDateTime.now();
        UUID requestedEventId = eventId(orderId, "REQUESTED");
        UUID confirmedEventId = eventId(orderId, "ASSET_RESERVATION_CONFIRMED");
        String metadataCanonical = serializeCanonical(Map.of(
                "correlationId", orderId.toString(),
                "userId", userId.toString()));
        OrderSubmissionRequestedV1 requested = new OrderSubmissionRequestedV1(
                orderId,
                userId,
                marketId,
                marketSequence,
                side,
                PRICE,
                AMOUNT,
                occurredAt);
        String requestedPayload = serializeCanonical(requested);
        String requestedHash = computeHash(
                requestedEventId,
                orderId,
                1,
                "OrderSubmissionRequestedV1",
                requestedPayload,
                metadataCanonical,
                occurredAt,
                GENESIS_HASH);
        OrderAssetReservationConfirmedV1 confirmed =
                new OrderAssetReservationConfirmedV1(orderId, userId, occurredAt);
        String confirmedPayload = serializeCanonical(confirmed);
        String confirmedHash = computeHash(
                confirmedEventId,
                orderId,
                2,
                "OrderAssetReservationConfirmedV1",
                confirmedPayload,
                metadataCanonical,
                occurredAt,
                requestedHash);

        headRows.add(new Object[] { orderId, confirmedEventId, confirmedHash, userId, AMOUNT });
        eventRows.add(new Object[] {
                requestedEventId,
                orderId,
                AGGREGATE_TYPE,
                1,
                "OrderSubmissionRequestedV1",
                requestedPayload,
                metadataCanonical,
                occurredAt,
                GENESIS_HASH,
                requestedHash
        });
        eventRows.add(new Object[] {
                confirmedEventId,
                orderId,
                AGGREGATE_TYPE,
                2,
                "OrderAssetReservationConfirmedV1",
                confirmedPayload,
                metadataCanonical,
                occurredAt,
                requestedHash,
                confirmedHash
        });
        matchingStateRows.add(new Object[] { orderId, userId, AMOUNT });
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
        SellBookWaitResult sellBookWait = waitForRedisSellBook(config, redisTemplate, rabbitAdmin);
        double orderbookAdmissionSeconds = sellPublish.elapsedSeconds() + sellBookWait.elapsedSeconds();

        System.out.printf("publishing %d incoming BUY confirmations directly to %s%n",
                config.events(), MATCH_ENGINE_ORDER_CONFIRMED_QUEUE);
        long started = System.nanoTime();
        PublishResult buyPublish = publishBuyToMatchEngine(config, rabbitTemplate, pairs);
        WaitResult waitResult = waitForDownstream(config, orderJdbc, walletJdbc, matchJdbc, rabbitAdmin, started);
        double elapsedSeconds = businessCompletionSeconds(waitResult, started);
        long activeReservationsAtBusinessCompletion = countRedisKeys(redisTemplate, "order:reservation:*");
        ReservationCleanupWaitResult reservationCleanupWait = waitForReservationCleanup(config, redisTemplate, started);

        long remainingSellOrders = zsetSize(redisTemplate, "orderbook:" + config.marketId() + ":sell");
        long remainingBuyOrders = zsetSize(redisTemplate, "orderbook:" + config.marketId() + ":buy");
        printResult(
                config,
                sellPublish,
                sellBookWait,
                orderbookAdmissionSeconds,
                buyPublish,
                waitResult,
                elapsedSeconds,
                activeReservationsAtBusinessCompletion,
                reservationCleanupWait,
                remainingSellOrders,
                remainingBuyOrders);

        require(sellPublish.failures() == 0, "SELL publish should have no failures");
        require(buyPublish.failures() == 0, "BUY publish should have no failures");
        require(waitResult.orderCommandMatchedRows() == config.events() * 2L,
                "Order command state should mark buyer and seller orders as MATCHED");
        require(waitResult.tradeExecutions() == config.events(), "MatchEngine should persist one TradeExecuted per match");
        require(waitResult.completedTrades() == config.events(),
                "Durable trade convergence should include MatchEngine execution, Order trade application, and Wallet settlement");
        require(waitResult.walletTradeSettlements() == config.events(), "Wallet should settle every TradeExecuted exactly once");
        require(waitResult.lockedCurrency() == 0, "Wallet buyer locked currency should be released");
        require(waitResult.lockedAmount() == 0, "Wallet seller locked amount should be released");
        require(waitResult.buyerAvailableAmount() == config.events() * (long) AMOUNT, "Wallet buyers should receive energy");
        require(waitResult.sellerAvailableCurrency() == config.events() * (long) PRICE * AMOUNT, "Wallet sellers should receive currency");
        require(remainingSellOrders == 0, "all resting SELL orders should be consumed");
        require(remainingBuyOrders == 0, "incoming BUY orders should not remain in order book");
        require(reservationCleanupWait.activeReservations() == 0, "MatchEngine reservations should converge to zero");
    }

    private static double businessCompletionSeconds(WaitResult waitResult, long startedNanos) {
        double nowSeconds = (System.nanoTime() - startedNanos) / 1_000_000_000.0;
        double completedTradesSeconds = waitResult.completedTradesReachedSeconds() > 0
                ? waitResult.completedTradesReachedSeconds()
                : nowSeconds;
        double fullyDrainedSeconds = waitResult.queueFullyDrainedSeconds() > 0
                ? waitResult.queueFullyDrainedSeconds()
                : nowSeconds;
        return Math.max(completedTradesSeconds, fullyDrainedSeconds);
    }

    private static ReservationCleanupWaitResult waitForReservationCleanup(
            Config config,
            RedisTemplate<String, String> redisTemplate,
            long startedNanos)
            throws InterruptedException {
        long deadline = startedNanos + TimeUnit.SECONDS.toNanos(config.timeoutSeconds());
        long activeReservations;
        do {
            activeReservations = countRedisKeys(redisTemplate, "order:reservation:*");
            double elapsedSeconds = (System.nanoTime() - startedNanos) / 1_000_000_000.0;
            if (activeReservations == 0) {
                return new ReservationCleanupWaitResult(0, elapsedSeconds);
            }
            TimeUnit.MILLISECONDS.sleep(100);
        } while (System.nanoTime() < deadline);

        activeReservations = countRedisKeys(redisTemplate, "order:reservation:*");
        double elapsedSeconds = (System.nanoTime() - startedNanos) / 1_000_000_000.0;
        return new ReservationCleanupWaitResult(activeReservations, elapsedSeconds);
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
        LongAdder acquireWaitNanos = new LongAdder();
        LongAdder sendNanos = new LongAdder();
        AtomicLong maxAcquireWaitNanos = new AtomicLong();
        AtomicLong maxSendNanos = new AtomicLong();
        AtomicLong maxScheduleLagNanos = new AtomicLong();
        CountDownLatch done = new CountDownLatch(pairs.size());
        ExecutorService executor = Executors.newFixedThreadPool(config.publishers());
        Semaphore inFlight = new Semaphore(config.publishers() * 2);

        long started = System.nanoTime();
        long intervalNanos = targetTps > 0 ? Math.max(1L, 1_000_000_000L / targetTps) : 0L;
        long scheduledIndex = 0;
        for (Pair pair : pairs) {
            if (intervalNanos > 0) {
                long scheduledNanos = started + intervalNanos * scheduledIndex;
                sleepUntil(scheduledNanos);
                updateMax(maxScheduleLagNanos, Math.max(0, System.nanoTime() - scheduledNanos));
                scheduledIndex++;
            }
            long acquireStarted = System.nanoTime();
            inFlight.acquire();
            long acquireElapsed = System.nanoTime() - acquireStarted;
            acquireWaitNanos.add(acquireElapsed);
            updateMax(maxAcquireWaitNanos, acquireElapsed);
            executor.execute(() -> {
                try {
                    OrderConfirmedEvent event = sell ? pair.sellConfirmed(config.marketId()) : pair.buyConfirmed(config.marketId());
                    long sendStarted = System.nanoTime();
                    rabbitTemplate.convertAndSend("", MATCH_ENGINE_ORDER_CONFIRMED_QUEUE, event);
                    long sendElapsed = System.nanoTime() - sendStarted;
                    sendNanos.add(sendElapsed);
                    updateMax(maxSendNanos, sendElapsed);
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
        return new PublishResult(
                published.get(),
                failures.get(),
                elapsedSeconds,
                targetTps,
                acquireWaitNanos.sum() / 1_000_000_000.0,
                maxAcquireWaitNanos.get() / 1_000_000.0,
                sendNanos.sum() / 1_000_000_000.0,
                maxSendNanos.get() / 1_000_000.0,
                maxScheduleLagNanos.get() / 1_000_000.0);
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

    private static void updateMax(AtomicLong target, long value) {
        long current;
        do {
            current = target.get();
            if (value <= current) {
                return;
            }
        } while (!target.compareAndSet(current, value));
    }

    private static SellBookWaitResult waitForRedisSellBook(
            Config config,
            RedisTemplate<String, String> redisTemplate,
            RabbitAdmin rabbitAdmin) throws InterruptedException {
        String key = "orderbook:" + config.marketId() + ":sell";
        long started = System.nanoTime();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.timeoutSeconds());
        long latestSize = 0;
        while (System.nanoTime() < deadline) {
            latestSize = zsetSize(redisTemplate, key);
            if (latestSize == config.events()) {
                double elapsedSeconds = (System.nanoTime() - started) / 1_000_000_000.0;
                return new SellBookWaitResult(latestSize, elapsedSeconds);
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
        long nextDbSnapshotNanos = 0;
        double tradeExecutionsReachedSeconds = -1;
        double orderMatchedReachedSeconds = -1;
        double walletSettlementsReachedSeconds = -1;
        double completedTradesReachedSeconds = -1;
        double strictCompletedTradesReachedSeconds = -1;
        double projectionCaughtUpSeconds = -1;
        long maxMatchEngineQueueReady = 0;
        long maxOrderTradeExecutedQueueReady = 0;
        long maxWalletTradeExecutedQueueReady = 0;
        long maxOrderTradeAppliedQueueReady = 0;
        long maxWalletTradeSettledQueueReady = 0;
        long maxMatchEngineQueueUnacked = 0;
        long maxOrderTradeExecutedQueueUnacked = 0;
        long maxWalletTradeExecutedQueueUnacked = 0;
        long maxOrderTradeAppliedQueueUnacked = 0;
        long maxWalletTradeSettledQueueUnacked = 0;
        int consecutiveReadyDrainedSamples = 0;
        int consecutiveFullyDrainedSamples = 0;
        double queueReadyDrainedSeconds = -1;
        double queueFullyDrainedSeconds = -1;
        QueueDrainTracker queueDrainTracker = new QueueDrainTracker();
        do {
            WaitResult queueLatest = queueSnapshot(config, rabbitAdmin);
            double elapsedSeconds = (System.nanoTime() - startedNanos) / 1_000_000_000.0;
            queueDrainTracker.record(queueLatest, elapsedSeconds);
            maxMatchEngineQueueReady = Math.max(maxMatchEngineQueueReady, queueLatest.matchEngineQueueReady());
            maxOrderTradeExecutedQueueReady = Math.max(maxOrderTradeExecutedQueueReady, queueLatest.orderTradeExecutedQueueReady());
            maxWalletTradeExecutedQueueReady = Math.max(maxWalletTradeExecutedQueueReady, queueLatest.walletTradeExecutedQueueReady());
            maxOrderTradeAppliedQueueReady = Math.max(maxOrderTradeAppliedQueueReady, queueLatest.orderTradeAppliedQueueReady());
            maxWalletTradeSettledQueueReady = Math.max(maxWalletTradeSettledQueueReady, queueLatest.walletTradeSettledQueueReady());
            maxMatchEngineQueueUnacked = Math.max(maxMatchEngineQueueUnacked, queueLatest.matchEngineQueueUnacked());
            maxOrderTradeExecutedQueueUnacked = Math.max(maxOrderTradeExecutedQueueUnacked, queueLatest.orderTradeExecutedQueueUnacked());
            maxWalletTradeExecutedQueueUnacked = Math.max(maxWalletTradeExecutedQueueUnacked, queueLatest.walletTradeExecutedQueueUnacked());
            maxOrderTradeAppliedQueueUnacked = Math.max(maxOrderTradeAppliedQueueUnacked, queueLatest.orderTradeAppliedQueueUnacked());
            maxWalletTradeSettledQueueUnacked = Math.max(maxWalletTradeSettledQueueUnacked, queueLatest.walletTradeSettledQueueUnacked());
            if (queuesReadyDrained(queueLatest)) {
                consecutiveReadyDrainedSamples++;
            } else {
                consecutiveReadyDrainedSamples = 0;
            }
            if (queueReadyDrainedSeconds < 0 && consecutiveReadyDrainedSamples >= 3) {
                queueReadyDrainedSeconds = elapsedSeconds;
            }
            if (queuesFullyDrained(queueLatest)) {
                consecutiveFullyDrainedSamples++;
            } else {
                consecutiveFullyDrainedSamples = 0;
            }
            if (consecutiveFullyDrainedSamples >= 3) {
                queueFullyDrainedSeconds = elapsedSeconds;
            }
            long now = System.nanoTime();
            boolean shouldSnapshotDb = now >= nextDbSnapshotNanos || consecutiveFullyDrainedSamples >= 3;
            if (shouldSnapshotDb) {
                latest = progressSnapshot(config, orderJdbc, walletJdbc, matchJdbc, queueLatest);
                nextDbSnapshotNanos = now + TimeUnit.SECONDS.toNanos(1);
                if (tradeExecutionsReachedSeconds < 0 && latest.tradeExecutions() == config.events()) {
                    tradeExecutionsReachedSeconds = elapsedSeconds;
                }
                if (orderMatchedReachedSeconds < 0 && latest.orderCommandMatchedRows() == config.events() * 2L) {
                    orderMatchedReachedSeconds = elapsedSeconds;
                }
                if (walletSettlementsReachedSeconds < 0 && latest.walletTradeSettlements() == config.events()) {
                    walletSettlementsReachedSeconds = elapsedSeconds;
                }
                if (completedTradesReachedSeconds < 0 && latest.completedTrades() == config.events()) {
                    completedTradesReachedSeconds = elapsedSeconds;
                }
                if (strictCompletedTradesReachedSeconds < 0 && latest.completedTrades() == config.events()) {
                    strictCompletedTradesReachedSeconds = elapsedSeconds;
                }
                if (projectionCaughtUpSeconds < 0 && projectionSatisfied(config, latest)) {
                    projectionCaughtUpSeconds = elapsedSeconds;
                }
                if (coreBusinessCountsReached(config, latest) && consecutiveFullyDrainedSamples >= 3) {
                    WaitResult verified = snapshot(config, orderJdbc, walletJdbc, matchJdbc, rabbitAdmin);
                    if (!invariantsSatisfied(config, verified)) {
                        latest = verified;
                        TimeUnit.MILLISECONDS.sleep(100);
                        continue;
                    }
                    if (completedTradesReachedSeconds < 0 && verified.completedTrades() == config.events()) {
                        completedTradesReachedSeconds = elapsedSeconds;
                    }
                    if (strictCompletedTradesReachedSeconds < 0 && verified.completedTrades() == config.events()) {
                        strictCompletedTradesReachedSeconds = elapsedSeconds;
                    }
                    if (projectionCaughtUpSeconds < 0 && projectionSatisfied(config, verified)) {
                        projectionCaughtUpSeconds = elapsedSeconds;
                    }
                    double projectionLagSeconds = projectionCaughtUpSeconds >= 0 && completedTradesReachedSeconds >= 0
                            ? Math.max(0.0, projectionCaughtUpSeconds - completedTradesReachedSeconds)
                            : -1;
                    return verified.withDiagnostics(
                            tradeExecutionsReachedSeconds,
                            orderMatchedReachedSeconds,
                            walletSettlementsReachedSeconds,
                            completedTradesReachedSeconds,
                            strictCompletedTradesReachedSeconds,
                            queueReadyDrainedSeconds,
                            queueFullyDrainedSeconds,
                            projectionCaughtUpSeconds,
                            projectionLagSeconds,
                            maxMatchEngineQueueReady,
                            maxOrderTradeExecutedQueueReady,
                            maxWalletTradeExecutedQueueReady,
                            maxOrderTradeAppliedQueueReady,
                            maxWalletTradeSettledQueueReady,
                            maxMatchEngineQueueUnacked,
                            maxOrderTradeExecutedQueueUnacked,
                            maxWalletTradeExecutedQueueUnacked,
                            maxOrderTradeAppliedQueueUnacked,
                            maxWalletTradeSettledQueueUnacked,
                            queueDrainTracker.snapshot());
                }
            }
            TimeUnit.MILLISECONDS.sleep(100);
        } while (System.nanoTime() < deadline);

        WaitResult finalSnapshot = snapshot(config, orderJdbc, walletJdbc, matchJdbc, rabbitAdmin);
        return finalSnapshot.withDiagnostics(
                tradeExecutionsReachedSeconds,
                orderMatchedReachedSeconds,
                walletSettlementsReachedSeconds,
                completedTradesReachedSeconds,
                strictCompletedTradesReachedSeconds,
                queueReadyDrainedSeconds,
                queueFullyDrainedSeconds,
                projectionCaughtUpSeconds,
                projectionCaughtUpSeconds >= 0 && completedTradesReachedSeconds >= 0
                        ? Math.max(0.0, projectionCaughtUpSeconds - completedTradesReachedSeconds)
                        : -1,
                maxMatchEngineQueueReady,
                maxOrderTradeExecutedQueueReady,
                maxWalletTradeExecutedQueueReady,
                maxOrderTradeAppliedQueueReady,
                maxWalletTradeSettledQueueReady,
                maxMatchEngineQueueUnacked,
                maxOrderTradeExecutedQueueUnacked,
                maxWalletTradeExecutedQueueUnacked,
                maxOrderTradeAppliedQueueUnacked,
                maxWalletTradeSettledQueueUnacked,
                queueDrainTracker.snapshot());
    }

    private static boolean queuesReadyDrained(WaitResult latest) {
        return latest.matchEngineQueueReady() == 0
                && latest.orderTradeExecutedQueueReady() == 0
                && latest.walletTradeExecutedQueueReady() == 0
                && latest.orderTradeAppliedQueueReady() == 0
                && latest.walletTradeSettledQueueReady() == 0;
    }

    private static boolean queuesFullyDrained(WaitResult latest) {
        return queuesReadyDrained(latest)
                && latest.matchEngineQueueUnacked() == 0
                && latest.orderTradeExecutedQueueUnacked() == 0
                && latest.walletTradeExecutedQueueUnacked() == 0
                && latest.orderTradeAppliedQueueUnacked() == 0
                && latest.walletTradeSettledQueueUnacked() == 0;
    }

    private static boolean invariantsSatisfied(Config config, WaitResult verified) {
        return verified.orderMatchedEvents() == config.events() * 2L
                && verified.orderCommandMatchedRows() == config.events() * 2L
                && verified.tradeExecutions() == config.events()
                && verified.completedTrades() == config.events()
                && verified.walletTradeSettlements() == config.events()
                && verified.lockedCurrency() == 0
                && verified.lockedAmount() == 0
                && verified.buyerAvailableAmount() == config.events() * (long) AMOUNT
                && verified.sellerAvailableCurrency() == config.events() * (long) PRICE * AMOUNT;
    }

    private static boolean coreBusinessCountsReached(Config config, WaitResult latest) {
        return latest.orderMatchedEvents() == config.events() * 2L
                && latest.orderCommandMatchedRows() == config.events() * 2L
                && latest.tradeExecutions() == config.events()
                && latest.completedTrades() == config.events()
                && latest.walletTradeSettlements() == config.events();
    }

    private static boolean projectionSatisfied(Config config, WaitResult verified) {
        return verified.orderCurrentMatchedRows() == config.events() * 2L
                && verified.orderProjectionStaleRows() == 0;
    }

    private static WaitResult queueSnapshot(Config config, RabbitAdmin rabbitAdmin) {
        QueueDepth matchEngineOrderConfirmed = queueDepth(config, rabbitAdmin, MATCH_ENGINE_ORDER_CONFIRMED_QUEUE);
        QueueDepth orderTradeExecuted = queueDepth(config, rabbitAdmin, ORDER_TRADE_EXECUTED_QUEUE);
        QueueDepth walletTradeExecuted = queueDepth(config, rabbitAdmin, WALLET_TRADE_EXECUTED_QUEUE);
        QueueDepth orderTradeApplied = queueDepth(config, rabbitAdmin, MATCH_ENGINE_ORDER_TRADE_APPLIED_QUEUE);
        QueueDepth walletTradeSettled = queueDepth(config, rabbitAdmin, MATCH_ENGINE_WALLET_TRADE_SETTLED_QUEUE);
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
                -1,
                -1,
                matchEngineOrderConfirmed.ready(),
                orderTradeExecuted.ready(),
                walletTradeExecuted.ready(),
                orderTradeApplied.ready(),
                walletTradeSettled.ready(),
                matchEngineOrderConfirmed.unacked(),
                orderTradeExecuted.unacked(),
                walletTradeExecuted.unacked(),
                orderTradeApplied.unacked(),
                walletTradeSettled.unacked(),
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                EMPTY_QUEUE_DRAIN_DIAGNOSTICS);
    }

    private static WaitResult progressSnapshot(
            Config config,
            JdbcTemplate orderJdbc,
            JdbcTemplate walletJdbc,
            JdbcTemplate matchJdbc,
            WaitResult queueLatest) {
        long orderCommandMatchedRows = countOrderCommandMatchedRows(orderJdbc);
        return new WaitResult(
                orderCommandMatchedRows,
                orderCommandMatchedRows,
                -1,
                -1,
                -1,
                count(matchJdbc, "SELECT count(*) FROM match_engine.trade_executions"),
                countCompletedTrades(orderJdbc, walletJdbc, matchJdbc),
                count(walletJdbc, "SELECT count(*) FROM wallet_service.trade_settlements"),
                -1,
                -1,
                -1,
                -1,
                queueLatest.matchEngineQueueReady(),
                queueLatest.orderTradeExecutedQueueReady(),
                queueLatest.walletTradeExecutedQueueReady(),
                queueLatest.orderTradeAppliedQueueReady(),
                queueLatest.walletTradeSettledQueueReady(),
                queueLatest.matchEngineQueueUnacked(),
                queueLatest.orderTradeExecutedQueueUnacked(),
                queueLatest.walletTradeExecutedQueueUnacked(),
                queueLatest.orderTradeAppliedQueueUnacked(),
                queueLatest.walletTradeSettledQueueUnacked(),
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                EMPTY_QUEUE_DRAIN_DIAGNOSTICS);
    }

    private static WaitResult snapshot(
            Config config,
            JdbcTemplate orderJdbc,
            JdbcTemplate walletJdbc,
            JdbcTemplate matchJdbc,
            RabbitAdmin rabbitAdmin) {
        QueueDepth matchEngineOrderConfirmed = queueDepth(config, rabbitAdmin, MATCH_ENGINE_ORDER_CONFIRMED_QUEUE);
        QueueDepth orderTradeExecuted = queueDepth(config, rabbitAdmin, ORDER_TRADE_EXECUTED_QUEUE);
        QueueDepth walletTradeExecuted = queueDepth(config, rabbitAdmin, WALLET_TRADE_EXECUTED_QUEUE);
        QueueDepth orderTradeApplied = queueDepth(config, rabbitAdmin, MATCH_ENGINE_ORDER_TRADE_APPLIED_QUEUE);
        QueueDepth walletTradeSettled = queueDepth(config, rabbitAdmin, MATCH_ENGINE_WALLET_TRADE_SETTLED_QUEUE);
        long orderCommandMatchedRows = countOrderCommandMatchedRows(orderJdbc);
        return new WaitResult(
                orderCommandMatchedRows,
                orderCommandMatchedRows,
                count(orderJdbc, """
                        SELECT count(*)
                        FROM order_service.orders_current
                        WHERE status = 'MATCHED'
                          AND remaining_amount = 0
                          AND matched_amount = original_amount
                        """),
                count(orderJdbc, """
                        SELECT count(*)
                        FROM order_service.order_stream_heads h
                        JOIN order_service.orders_current oc ON oc.order_id = h.aggregate_id
                        WHERE h.current_version > oc.aggregate_version
                        """),
                count(orderJdbc, "SELECT count(*) FROM order_service.match_history"),
                count(matchJdbc, "SELECT count(*) FROM match_engine.trade_executions"),
                countCompletedTrades(orderJdbc, walletJdbc, matchJdbc),
                count(walletJdbc, "SELECT count(*) FROM wallet_service.trade_settlements"),
                count(walletJdbc, "SELECT COALESCE(sum(locked_currency), 0) FROM wallet_service.wallets"),
                count(walletJdbc, "SELECT COALESCE(sum(locked_amount), 0) FROM wallet_service.wallets"),
                count(walletJdbc, "SELECT COALESCE(sum(available_amount), 0) FROM wallet_service.wallets"),
                count(walletJdbc, "SELECT COALESCE(sum(available_currency), 0) FROM wallet_service.wallets"),
                matchEngineOrderConfirmed.ready(),
                orderTradeExecuted.ready(),
                walletTradeExecuted.ready(),
                orderTradeApplied.ready(),
                walletTradeSettled.ready(),
                matchEngineOrderConfirmed.unacked(),
                orderTradeExecuted.unacked(),
                walletTradeExecuted.unacked(),
                orderTradeApplied.unacked(),
                walletTradeSettled.unacked(),
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                -1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                EMPTY_QUEUE_DRAIN_DIAGNOSTICS);
    }

    private static long countOrderCommandMatchedRows(JdbcTemplate orderJdbc) {
        return count(orderJdbc, """
                SELECT count(*)
                FROM order_service.order_matching_state
                WHERE status = 'MATCHED'
                  AND remaining_amount = 0
                """);
    }

    private static long countCompletedTrades(
            JdbcTemplate orderJdbc,
            JdbcTemplate walletJdbc,
            JdbcTemplate matchJdbc) {
        long tradeExecutions = count(matchJdbc, "SELECT count(*) FROM match_engine.trade_executions");
        long orderTradeApplications = count(orderJdbc, "SELECT count(*) FROM order_service.order_trade_applications");
        long walletTradeSettlements = count(walletJdbc, "SELECT count(*) FROM wallet_service.trade_settlements");
        return Math.min(tradeExecutions, Math.min(orderTradeApplications, walletTradeSettlements));
    }

    private static void truncateOrderTestData(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute("""
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
                DO $$
                BEGIN
                    IF to_regclass('match_engine.trade_publish_checkpoints') IS NOT NULL THEN
                        TRUNCATE TABLE match_engine.trade_publish_checkpoints RESTART IDENTITY CASCADE;
                    END IF;
                    IF to_regclass('match_engine.reservation_cleanup_tasks') IS NOT NULL THEN
                        TRUNCATE TABLE match_engine.reservation_cleanup_tasks RESTART IDENTITY CASCADE;
                    END IF;
                END $$;
                """);
        jdbcTemplate.execute("""
                TRUNCATE TABLE
                    match_engine.trade_completion_markers,
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
        purgeIfPresent(rabbitAdmin, ORDER_TRADE_EXECUTED_QUEUE);
        purgeIfPresent(rabbitAdmin, WALLET_TRADE_EXECUTED_QUEUE);
        purgeIfPresent(rabbitAdmin, MATCH_ENGINE_ORDER_TRADE_APPLIED_QUEUE);
        purgeIfPresent(rabbitAdmin, MATCH_ENGINE_WALLET_TRADE_SETTLED_QUEUE);
        purgeIfPresent(rabbitAdmin, WALLET_AUCTION_BID_SUBMITTED_QUEUE);
        purgeIfPresent(rabbitAdmin, WALLET_AUCTION_CLEARED_QUEUE);
        purgeIfPresent(rabbitAdmin, ORDER_AUCTION_CLEARED_QUEUE);
        purgeIfPresent(rabbitAdmin, ORDER_AUCTION_CREATED_QUEUE);
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
            keys.add("order:reservation:" + pair.buyOrderId());
            keys.add("order:reservation:" + pair.sellOrderId());
            keys.add("user:" + pair.buyerId() + ":orders");
            keys.add("user:" + pair.sellerId() + ":orders");
        }
        redisTemplate.delete(keys);
        deleteRedisKeys(redisTemplate, "order:reservation:*");
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

    private static UUID eventId(UUID orderId, String discriminator) {
        return UUID.nameUUIDFromBytes((orderId + ":" + discriminator).getBytes(StandardCharsets.UTF_8));
    }

    private static String serializeCanonical(Object value) {
        try {
            return CANONICAL_OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new IllegalArgumentException("Seed payload cannot be serialized", e);
        }
    }

    private static String computeHash(
            UUID eventId,
            UUID aggregateId,
            long aggregateVersion,
            String eventType,
            String payloadCanonical,
            String metadataCanonical,
            LocalDateTime occurredAt,
            String prevHash) {
        String material = eventId + "|"
                + aggregateId + "|"
                + aggregateVersion + "|"
                + eventType + "|"
                + payloadCanonical + "|"
                + metadataCanonical + "|"
                + 1 + "|"
                + occurredAt + "|"
                + prevHash;
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256")
                            .digest(material.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static CachingConnectionFactory rabbitConnectionFactory(Config config) {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(config.rabbitHost(), config.rabbitPort());
        connectionFactory.setUsername(config.rabbitUser());
        connectionFactory.setPassword(config.rabbitPassword());
        connectionFactory.setChannelCacheSize(config.publisherChannelCacheSize());
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

    private static long countRedisKeys(RedisTemplate<String, String> redisTemplate, String pattern) {
        Long count = redisTemplate.execute((RedisCallback<Long>) connection -> {
            long matched = 0;
            ScanOptions options = ScanOptions.scanOptions()
                    .match(pattern)
                    .count(1_000)
                    .build();
            try (Cursor<byte[]> cursor = connection.scan(options)) {
                while (cursor.hasNext()) {
                    cursor.next();
                    matched++;
                }
            } catch (RuntimeException e) {
                throw new IllegalStateException("Failed to scan Redis keys for pattern " + pattern, e);
            }
            return matched;
        });
        return count == null ? 0L : count;
    }

    private static void deleteRedisKeys(RedisTemplate<String, String> redisTemplate, String pattern) {
        List<String> keys = redisTemplate.execute((RedisCallback<List<String>>) connection -> {
            List<String> matched = new ArrayList<>();
            ScanOptions options = ScanOptions.scanOptions()
                    .match(pattern)
                    .count(1_000)
                    .build();
            try (Cursor<byte[]> cursor = connection.scan(options)) {
                while (cursor.hasNext()) {
                    matched.add(new String(cursor.next(), StandardCharsets.UTF_8));
                }
            } catch (RuntimeException e) {
                throw new IllegalStateException("Failed to scan Redis keys for pattern " + pattern, e);
            }
            return matched;
        });
        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }
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

    private static QueueDepth queueDepth(Config config, RabbitAdmin rabbitAdmin, String queueName) {
        try {
            String encodedQueue = URLEncoder.encode(queueName, StandardCharsets.UTF_8).replace("+", "%20");
            String basicAuth = Base64.getEncoder().encodeToString(
                    (config.rabbitUser() + ":" + config.rabbitPassword()).getBytes(StandardCharsets.UTF_8));
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + config.rabbitHost() + ":" + config.rabbitManagementPort()
                            + "/api/queues/%2F/" + encodedQueue
                            + "?disable_stats=true&enable_queue_totals=true"))
                    .header("Authorization", "Basic " + basicAuth)
                    .GET()
                    .build();
            HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                return new QueueDepth(queueReady(rabbitAdmin, queueName), 0, queueReady(rabbitAdmin, queueName));
            }
            JsonNode json = METRICS_OBJECT_MAPPER.readTree(response.body());
            long ready = json.path("messages_ready").asLong(0);
            long unacked = json.path("messages_unacknowledged").asLong(0);
            long total = json.path("messages").asLong(ready + unacked);
            return new QueueDepth(ready, unacked, total);
        } catch (IOException | InterruptedException | RuntimeException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            long ready = queueReady(rabbitAdmin, queueName);
            return new QueueDepth(ready, 0, ready);
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
            SellBookWaitResult sellBookWait,
            double orderbookAdmissionSeconds,
            PublishResult buyPublish,
            WaitResult waitResult,
            double elapsedSeconds,
            long activeReservationsAtBusinessCompletion,
            ReservationCleanupWaitResult reservationCleanupWait,
            long remainingSellOrders,
            long remainingBuyOrders) {
        System.out.println("{");
        System.out.printf("  \"mode\": \"matchedE2e\",%n");
        System.out.printf("  \"marketId\": \"%s\",%n", config.marketId());
        System.out.printf("  \"matches\": %d,%n", config.events());
        System.out.printf("  \"publishers\": %d,%n", config.publishers());
        System.out.printf("  \"publisherChannelCacheSize\": %d,%n", config.publisherChannelCacheSize());
        System.out.printf("  \"targetTps\": %d,%n", config.targetTps());
        System.out.printf("  \"durationSeconds\": %d,%n", config.durationSeconds());
        System.out.printf("  \"expectedBuyPublishSeconds\": %.2f,%n", config.expectedBuyPublishSeconds());
        System.out.printf("  \"sellPublished\": %d,%n", sellPublish.published());
        System.out.printf("  \"sellPublishFailures\": %d,%n", sellPublish.failures());
        System.out.printf("  \"sellPublishSeconds\": %.2f,%n", sellPublish.elapsedSeconds());
        System.out.printf("  \"sellBookReachedOrders\": %d,%n", sellBookWait.readyOrders());
        System.out.printf("  \"sellBookPostPublishWaitSeconds\": %.2f,%n", sellBookWait.elapsedSeconds());
        System.out.printf("  \"orderbookAdmissionSeconds\": %.2f,%n", orderbookAdmissionSeconds);
        System.out.printf("  \"buyPublished\": %d,%n", buyPublish.published());
        System.out.printf("  \"buyPublishFailures\": %d,%n", buyPublish.failures());
        System.out.printf("  \"buyPublishSeconds\": %.2f,%n", buyPublish.elapsedSeconds());
        System.out.printf("  \"sellPublishAcquireWaitSeconds\": %.4f,%n", sellPublish.acquireWaitSeconds());
        System.out.printf("  \"sellPublishMaxAcquireWaitMs\": %.3f,%n", sellPublish.maxAcquireWaitMs());
        System.out.printf("  \"sellPublishSendSeconds\": %.4f,%n", sellPublish.sendSeconds());
        System.out.printf("  \"sellPublishMaxSendMs\": %.3f,%n", sellPublish.maxSendMs());
        System.out.printf("  \"sellPublishMaxScheduleLagMs\": %.3f,%n", sellPublish.maxScheduleLagMs());
        System.out.printf("  \"buyPublishAcquireWaitSeconds\": %.4f,%n", buyPublish.acquireWaitSeconds());
        System.out.printf("  \"buyPublishMaxAcquireWaitMs\": %.3f,%n", buyPublish.maxAcquireWaitMs());
        System.out.printf("  \"buyPublishSendSeconds\": %.4f,%n", buyPublish.sendSeconds());
        System.out.printf("  \"buyPublishMaxSendMs\": %.3f,%n", buyPublish.maxSendMs());
        System.out.printf("  \"buyPublishMaxScheduleLagMs\": %.3f,%n", buyPublish.maxScheduleLagMs());
        double actualBuyPublishTps = buyPublish.published() / Math.max(buyPublish.elapsedSeconds(), 0.001);
        double offeredLoadRatio = config.targetTps() > 0 ? actualBuyPublishTps / config.targetTps() : 1.0;
        long finalQueueBacklog = finalQueueBacklog(waitResult);
        List<String> capacityInvalidReasons = capacityInvalidReasons(
                config,
                sellPublish,
                buyPublish,
                waitResult,
                remainingSellOrders,
                remainingBuyOrders,
                reservationCleanupWait.activeReservations(),
                offeredLoadRatio,
                finalQueueBacklog);
        System.out.printf("  \"businessInputOrderTps\": %.2f,%n", actualBuyPublishTps);
        System.out.printf("  \"minOfferedLoadRatio\": %.4f,%n", config.minOfferedLoadRatio());
        System.out.printf("  \"offeredLoadRatio\": %.4f,%n", offeredLoadRatio);
        System.out.printf("  \"validForCapacityComparison\": %s,%n", capacityInvalidReasons.isEmpty());
        System.out.print("  \"capacityInvalidReasons\": ");
        printJsonStringArray(capacityInvalidReasons);
        System.out.println(",");
        System.out.printf("  \"drainSecondsAfterBuyPublish\": %.2f,%n", Math.max(0.0, elapsedSeconds - buyPublish.elapsedSeconds()));
        System.out.printf("  \"elapsedSeconds\": %.2f,%n", elapsedSeconds);
        double businessCompletedTradeTps = waitResult.completedTrades() / Math.max(elapsedSeconds, 0.001);
        long blendedMarketFlowOrders = (long) sellPublish.published() + buyPublish.published();
        double blendedMarketFlowSeconds = orderbookAdmissionSeconds + elapsedSeconds;
        System.out.printf("  \"businessOrderbookAdmissionTps\": %.2f,%n",
                sellBookWait.readyOrders() / Math.max(orderbookAdmissionSeconds, 0.001));
        System.out.printf("  \"businessCompletedTradeTps\": %.2f,%n", businessCompletedTradeTps);
        System.out.printf("  \"businessMarketFlowOrders\": %d,%n", blendedMarketFlowOrders);
        System.out.printf("  \"businessMarketFlowSeconds\": %.2f,%n", blendedMarketFlowSeconds);
        System.out.printf("  \"businessMarketFlowTps\": %.2f,%n",
                blendedMarketFlowOrders / Math.max(blendedMarketFlowSeconds, 0.001));
        System.out.printf("  \"businessCompletionSeconds\": %.2f,%n", elapsedSeconds);
        System.out.printf("  \"activeReservationsAtBusinessCompletion\": %d,%n", activeReservationsAtBusinessCompletion);
        System.out.printf("  \"reservationCleanupReachedSeconds\": %.2f,%n", reservationCleanupWait.reachedSeconds());
        System.out.printf("  \"reservationCleanupTailAfterBusinessSeconds\": %.2f,%n",
                Math.max(0.0, reservationCleanupWait.reachedSeconds() - elapsedSeconds));
        System.out.printf("  \"projectionIncludedInBusinessGate\": false,%n");
        System.out.printf("  \"queueReadyDrainedSeconds\": %.2f,%n", waitResult.queueReadyDrainedSeconds());
        System.out.printf("  \"queueFullyDrainedSeconds\": %.2f,%n", waitResult.queueFullyDrainedSeconds());
        System.out.printf("  \"tradeExecutionsReachedSeconds\": %.2f,%n", waitResult.tradeExecutionsReachedSeconds());
        System.out.printf("  \"matchEngineTradeExecutionReachTps\": %.2f,%n", rate(config.events(), waitResult.tradeExecutionsReachedSeconds()));
        System.out.printf("  \"orderMatchedReachedSeconds\": %.2f,%n", waitResult.orderMatchedReachedSeconds());
        System.out.printf("  \"orderTradeApplicationReachTps\": %.2f,%n", rate(config.events(), waitResult.orderMatchedReachedSeconds()));
        System.out.printf("  \"walletSettlementsReachedSeconds\": %.2f,%n", waitResult.walletSettlementsReachedSeconds());
        System.out.printf("  \"walletTradeSettlementReachTps\": %.2f,%n", rate(config.events(), waitResult.walletSettlementsReachedSeconds()));
        System.out.printf("  \"completedTradesReachedSeconds\": %.2f,%n", waitResult.completedTradesReachedSeconds());
        System.out.printf("  \"businessConvergenceReachTps\": %.2f,%n", rate(config.events(), waitResult.completedTradesReachedSeconds()));
        System.out.printf("  \"strictCompletedTradesReachedSeconds\": %.2f,%n", waitResult.strictCompletedTradesReachedSeconds());
        System.out.printf("  \"strictBusinessConvergenceReachTps\": %.2f,%n", rate(config.events(), waitResult.strictCompletedTradesReachedSeconds()));
        System.out.printf("  \"orderProjectionCaughtUpSeconds\": %.2f,%n", waitResult.orderProjectionCaughtUpSeconds());
        System.out.printf("  \"orderProjectionLagSeconds\": %.2f,%n", waitResult.orderProjectionLagSeconds());
        System.out.printf("  \"orderMatchedEvents\": %d,%n", waitResult.orderMatchedEvents());
        System.out.printf("  \"orderCommandMatchedRows\": %d,%n", waitResult.orderCommandMatchedRows());
        System.out.printf("  \"orderCurrentMatchedRows\": %d,%n", waitResult.orderCurrentMatchedRows());
        System.out.printf("  \"orderProjectionStaleRows\": %d,%n", waitResult.orderProjectionStaleRows());
        System.out.printf("  \"orderProjectionCompletionRatio\": %.4f,%n", ratio(waitResult.orderCurrentMatchedRows(), config.events() * 2L));
        System.out.printf("  \"orderProjectionStaleRatio\": %.4f,%n", ratio(waitResult.orderProjectionStaleRows(), config.events() * 2L));
        System.out.printf("  \"matchHistoryRows\": %d,%n", waitResult.matchHistoryRows());
        System.out.printf("  \"tradeExecutions\": %d,%n", waitResult.tradeExecutions());
        System.out.printf("  \"completedTrades\": %d,%n", waitResult.completedTrades());
        System.out.printf("  \"walletTradeSettlements\": %d,%n", waitResult.walletTradeSettlements());
        System.out.printf("  \"lockedCurrency\": %d,%n", waitResult.lockedCurrency());
        System.out.printf("  \"lockedAmount\": %d,%n", waitResult.lockedAmount());
        System.out.printf("  \"buyerAvailableAmount\": %d,%n", waitResult.buyerAvailableAmount());
        System.out.printf("  \"sellerAvailableCurrency\": %d,%n", waitResult.sellerAvailableCurrency());
        System.out.printf("  \"matchEngineQueueReady\": %d,%n", waitResult.matchEngineQueueReady());
        System.out.printf("  \"orderTradeExecutedQueueReady\": %d,%n", waitResult.orderTradeExecutedQueueReady());
        System.out.printf("  \"walletTradeExecutedQueueReady\": %d,%n", waitResult.walletTradeExecutedQueueReady());
        System.out.printf("  \"orderTradeAppliedQueueReady\": %d,%n", waitResult.orderTradeAppliedQueueReady());
        System.out.printf("  \"walletTradeSettledQueueReady\": %d,%n", waitResult.walletTradeSettledQueueReady());
        System.out.printf("  \"matchEngineQueueUnacked\": %d,%n", waitResult.matchEngineQueueUnacked());
        System.out.printf("  \"orderTradeExecutedQueueUnacked\": %d,%n", waitResult.orderTradeExecutedQueueUnacked());
        System.out.printf("  \"walletTradeExecutedQueueUnacked\": %d,%n", waitResult.walletTradeExecutedQueueUnacked());
        System.out.printf("  \"orderTradeAppliedQueueUnacked\": %d,%n", waitResult.orderTradeAppliedQueueUnacked());
        System.out.printf("  \"walletTradeSettledQueueUnacked\": %d,%n", waitResult.walletTradeSettledQueueUnacked());
        System.out.printf("  \"maxMatchEngineQueueReady\": %d,%n", waitResult.maxMatchEngineQueueReady());
        System.out.printf("  \"maxOrderTradeExecutedQueueReady\": %d,%n", waitResult.maxOrderTradeExecutedQueueReady());
        System.out.printf("  \"maxWalletTradeExecutedQueueReady\": %d,%n", waitResult.maxWalletTradeExecutedQueueReady());
        System.out.printf("  \"maxOrderTradeAppliedQueueReady\": %d,%n", waitResult.maxOrderTradeAppliedQueueReady());
        System.out.printf("  \"maxWalletTradeSettledQueueReady\": %d,%n", waitResult.maxWalletTradeSettledQueueReady());
        System.out.printf("  \"maxMatchEngineQueueUnacked\": %d,%n", waitResult.maxMatchEngineQueueUnacked());
        System.out.printf("  \"maxOrderTradeExecutedQueueUnacked\": %d,%n", waitResult.maxOrderTradeExecutedQueueUnacked());
        System.out.printf("  \"maxWalletTradeExecutedQueueUnacked\": %d,%n", waitResult.maxWalletTradeExecutedQueueUnacked());
        System.out.printf("  \"maxOrderTradeAppliedQueueUnacked\": %d,%n", waitResult.maxOrderTradeAppliedQueueUnacked());
        System.out.printf("  \"maxWalletTradeSettledQueueUnacked\": %d,%n", waitResult.maxWalletTradeSettledQueueUnacked());
        System.out.printf("  \"lastNonZeroQueue\": \"%s\",%n", waitResult.queueDrainDiagnostics().lastNonZeroQueue());
        System.out.printf("  \"lastNonZeroQueueKind\": \"%s\",%n", waitResult.queueDrainDiagnostics().lastNonZeroKind());
        System.out.printf("  \"lastNonZeroQueueSeconds\": %.2f,%n", waitResult.queueDrainDiagnostics().lastNonZeroSeconds());
        System.out.printf("  \"lastNonZeroQueueReady\": %d,%n", waitResult.queueDrainDiagnostics().lastNonZeroReady());
        System.out.printf("  \"lastNonZeroQueueUnacked\": %d,%n", waitResult.queueDrainDiagnostics().lastNonZeroUnacked());
        System.out.printf("  \"queueDrainTailAfterCompletedTradesSeconds\": %.2f,%n",
                waitResult.completedTradesReachedSeconds() > 0 && waitResult.queueFullyDrainedSeconds() > 0
                        ? Math.max(0.0, waitResult.queueFullyDrainedSeconds() - waitResult.completedTradesReachedSeconds())
                        : -1);
        System.out.printf("  \"queueDrainTailAfterStrictCompletedTradesSeconds\": %.2f,%n",
                waitResult.strictCompletedTradesReachedSeconds() > 0 && waitResult.queueFullyDrainedSeconds() > 0
                        ? Math.max(0.0, waitResult.queueFullyDrainedSeconds() - waitResult.strictCompletedTradesReachedSeconds())
                        : -1);
        System.out.print("  \"queueDrainTimeline\": ");
        printQueueDrainTimeline(waitResult.queueDrainDiagnostics());
        System.out.println(",");
        System.out.printf("  \"finalQueueBacklog\": %d,%n", finalQueueBacklog);
        System.out.printf("  \"remainingSellOrders\": %d,%n", remainingSellOrders);
        System.out.printf("  \"remainingBuyOrders\": %d,%n", remainingBuyOrders);
        System.out.printf("  \"activeReservations\": %d%n", reservationCleanupWait.activeReservations());
        System.out.println("}");
    }

    private static List<String> capacityInvalidReasons(
            Config config,
            PublishResult sellPublish,
            PublishResult buyPublish,
            WaitResult waitResult,
            long remainingSellOrders,
            long remainingBuyOrders,
            long activeReservations,
            double offeredLoadRatio,
            long finalQueueBacklog) {
        List<String> reasons = new ArrayList<>();
        if (config.targetTps() > 0 && offeredLoadRatio < config.minOfferedLoadRatio()) {
            reasons.add("driver_offered_tps_below_threshold");
        }
        if (buyPublish.failures() != 0) {
            reasons.add("buy_publish_failures");
        }
        if (sellPublish.failures() != 0) {
            reasons.add("sell_publish_failures");
        }
        if (waitResult.completedTrades() != config.events()) {
            reasons.add("completed_trades_mismatch");
        }
        if (waitResult.tradeExecutions() != config.events()) {
            reasons.add("trade_executions_mismatch");
        }
        if (waitResult.walletTradeSettlements() != config.events()) {
            reasons.add("wallet_settlements_mismatch");
        }
        if (waitResult.orderCommandMatchedRows() != config.events() * 2L) {
            reasons.add("order_command_rows_mismatch");
        }
        if (remainingSellOrders != 0) {
            reasons.add("remaining_sell_orders");
        }
        if (remainingBuyOrders != 0) {
            reasons.add("remaining_buy_orders");
        }
        if (activeReservations != 0) {
            reasons.add("active_reservations");
        }
        if (finalQueueBacklog != 0) {
            reasons.add("final_queue_backlog");
        }
        return reasons;
    }

    private static long finalQueueBacklog(WaitResult waitResult) {
        return waitResult.matchEngineQueueReady()
                + waitResult.orderTradeExecutedQueueReady()
                + waitResult.walletTradeExecutedQueueReady()
                + waitResult.orderTradeAppliedQueueReady()
                + waitResult.walletTradeSettledQueueReady()
                + waitResult.matchEngineQueueUnacked()
                + waitResult.orderTradeExecutedQueueUnacked()
                + waitResult.walletTradeExecutedQueueUnacked()
                + waitResult.orderTradeAppliedQueueUnacked()
                + waitResult.walletTradeSettledQueueUnacked();
    }

    private static void printJsonStringArray(List<String> values) {
        System.out.print("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                System.out.print(", ");
            }
            System.out.printf("\"%s\"", values.get(i));
        }
        System.out.print("]");
    }

    private static void printQueueDrainTimeline(QueueDrainDiagnostics diagnostics) {
        System.out.print("{");
        List<QueueDrainState> queues = diagnostics.queues();
        for (int i = 0; i < queues.size(); i++) {
            QueueDrainState queue = queues.get(i);
            if (i > 0) {
                System.out.print(", ");
            }
            System.out.printf("\"%s\": {", queue.name());
            System.out.printf("\"lastReadyNonZeroSeconds\": %.2f, ", queue.lastReadyNonZeroSeconds());
            System.out.printf("\"lastReadyValue\": %d, ", queue.lastReadyValue());
            System.out.printf("\"lastUnackedNonZeroSeconds\": %.2f, ", queue.lastUnackedNonZeroSeconds());
            System.out.printf("\"lastUnackedValue\": %d, ", queue.lastUnackedValue());
            System.out.printf("\"lastAnyNonZeroSeconds\": %.2f, ", queue.lastAnyNonZeroSeconds());
            System.out.printf("\"lastAnyReady\": %d, ", queue.lastAnyReady());
            System.out.printf("\"lastAnyUnacked\": %d", queue.lastAnyUnacked());
            System.out.print("}");
        }
        System.out.print("}");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static double rate(long count, double seconds) {
        return seconds > 0 ? count / seconds : -1;
    }

    private static double ratio(long numerator, long denominator) {
        return denominator > 0 ? numerator / (double) denominator : -1;
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

    private record ReservationCleanupWaitResult(
            long activeReservations,
            double reachedSeconds) {
    }

    private record PublishResult(
            int published,
            int failures,
            double elapsedSeconds,
            int targetTps,
            double acquireWaitSeconds,
            double maxAcquireWaitMs,
            double sendSeconds,
            double maxSendMs,
            double maxScheduleLagMs) {
    }

    private record SellBookWaitResult(long readyOrders, double elapsedSeconds) {
    }

    private record QueueDepth(long ready, long unacked, long total) {
    }

    private static class QueueDrainTracker {

        private final List<MutableQueueDrainState> queues = List.of(
                new MutableQueueDrainState(MATCH_ENGINE_ORDER_CONFIRMED_QUEUE),
                new MutableQueueDrainState(ORDER_TRADE_EXECUTED_QUEUE),
                new MutableQueueDrainState(WALLET_TRADE_EXECUTED_QUEUE),
                new MutableQueueDrainState(MATCH_ENGINE_ORDER_TRADE_APPLIED_QUEUE),
                new MutableQueueDrainState(MATCH_ENGINE_WALLET_TRADE_SETTLED_QUEUE));
        private String lastNonZeroQueue = "none";
        private String lastNonZeroKind = "none";
        private double lastNonZeroSeconds = -1;
        private long lastNonZeroReady;
        private long lastNonZeroUnacked;

        void record(WaitResult latest, double elapsedSeconds) {
            recordQueue(0, latest.matchEngineQueueReady(), latest.matchEngineQueueUnacked(), elapsedSeconds);
            recordQueue(1, latest.orderTradeExecutedQueueReady(), latest.orderTradeExecutedQueueUnacked(), elapsedSeconds);
            recordQueue(2, latest.walletTradeExecutedQueueReady(), latest.walletTradeExecutedQueueUnacked(), elapsedSeconds);
            recordQueue(3, latest.orderTradeAppliedQueueReady(), latest.orderTradeAppliedQueueUnacked(), elapsedSeconds);
            recordQueue(4, latest.walletTradeSettledQueueReady(), latest.walletTradeSettledQueueUnacked(), elapsedSeconds);
        }

        private void recordQueue(int index, long ready, long unacked, double elapsedSeconds) {
            MutableQueueDrainState queue = queues.get(index);
            queue.record(ready, unacked, elapsedSeconds);
            if (ready > 0 || unacked > 0) {
                lastNonZeroQueue = queue.name;
                lastNonZeroKind = ready > 0 && unacked > 0 ? "ready+unacked" : ready > 0 ? "ready" : "unacked";
                lastNonZeroSeconds = elapsedSeconds;
                lastNonZeroReady = ready;
                lastNonZeroUnacked = unacked;
            }
        }

        QueueDrainDiagnostics snapshot() {
            return new QueueDrainDiagnostics(
                    queues.stream().map(MutableQueueDrainState::snapshot).toList(),
                    lastNonZeroQueue,
                    lastNonZeroKind,
                    lastNonZeroSeconds,
                    lastNonZeroReady,
                    lastNonZeroUnacked);
        }
    }

    private static class MutableQueueDrainState {

        private final String name;
        private double lastReadyNonZeroSeconds = -1;
        private long lastReadyValue;
        private double lastUnackedNonZeroSeconds = -1;
        private long lastUnackedValue;
        private double lastAnyNonZeroSeconds = -1;
        private long lastAnyReady;
        private long lastAnyUnacked;

        MutableQueueDrainState(String name) {
            this.name = name;
        }

        void record(long ready, long unacked, double elapsedSeconds) {
            if (ready > 0) {
                lastReadyNonZeroSeconds = elapsedSeconds;
                lastReadyValue = ready;
            }
            if (unacked > 0) {
                lastUnackedNonZeroSeconds = elapsedSeconds;
                lastUnackedValue = unacked;
            }
            if (ready > 0 || unacked > 0) {
                lastAnyNonZeroSeconds = elapsedSeconds;
                lastAnyReady = ready;
                lastAnyUnacked = unacked;
            }
        }

        QueueDrainState snapshot() {
            return new QueueDrainState(
                    name,
                    lastReadyNonZeroSeconds,
                    lastReadyValue,
                    lastUnackedNonZeroSeconds,
                    lastUnackedValue,
                    lastAnyNonZeroSeconds,
                    lastAnyReady,
                    lastAnyUnacked);
        }
    }

    private record QueueDrainState(
            String name,
            double lastReadyNonZeroSeconds,
            long lastReadyValue,
            double lastUnackedNonZeroSeconds,
            long lastUnackedValue,
            double lastAnyNonZeroSeconds,
            long lastAnyReady,
            long lastAnyUnacked) {
    }

    private record QueueDrainDiagnostics(
            List<QueueDrainState> queues,
            String lastNonZeroQueue,
            String lastNonZeroKind,
            double lastNonZeroSeconds,
            long lastNonZeroReady,
            long lastNonZeroUnacked) {
    }

    private record WaitResult(
            long orderMatchedEvents,
            long orderCommandMatchedRows,
            long orderCurrentMatchedRows,
            long orderProjectionStaleRows,
            long matchHistoryRows,
            long tradeExecutions,
            long completedTrades,
            long walletTradeSettlements,
            long lockedCurrency,
            long lockedAmount,
            long buyerAvailableAmount,
            long sellerAvailableCurrency,
            long matchEngineQueueReady,
            long orderTradeExecutedQueueReady,
            long walletTradeExecutedQueueReady,
            long orderTradeAppliedQueueReady,
            long walletTradeSettledQueueReady,
            long matchEngineQueueUnacked,
            long orderTradeExecutedQueueUnacked,
            long walletTradeExecutedQueueUnacked,
            long orderTradeAppliedQueueUnacked,
            long walletTradeSettledQueueUnacked,
            double tradeExecutionsReachedSeconds,
            double orderMatchedReachedSeconds,
            double walletSettlementsReachedSeconds,
            double completedTradesReachedSeconds,
            double strictCompletedTradesReachedSeconds,
            double queueReadyDrainedSeconds,
            double queueFullyDrainedSeconds,
            double orderProjectionCaughtUpSeconds,
            double orderProjectionLagSeconds,
            long maxMatchEngineQueueReady,
            long maxOrderTradeExecutedQueueReady,
            long maxWalletTradeExecutedQueueReady,
            long maxOrderTradeAppliedQueueReady,
            long maxWalletTradeSettledQueueReady,
            long maxMatchEngineQueueUnacked,
            long maxOrderTradeExecutedQueueUnacked,
            long maxWalletTradeExecutedQueueUnacked,
            long maxOrderTradeAppliedQueueUnacked,
            long maxWalletTradeSettledQueueUnacked,
            QueueDrainDiagnostics queueDrainDiagnostics) {
        WaitResult withDiagnostics(
                double tradeExecutionsReachedSeconds,
                double orderMatchedReachedSeconds,
                double walletSettlementsReachedSeconds,
                double completedTradesReachedSeconds,
                double strictCompletedTradesReachedSeconds,
                double queueReadyDrainedSeconds,
                double queueFullyDrainedSeconds,
                double orderProjectionCaughtUpSeconds,
                double orderProjectionLagSeconds,
                long maxMatchEngineQueueReady,
                long maxOrderTradeExecutedQueueReady,
                long maxWalletTradeExecutedQueueReady,
                long maxOrderTradeAppliedQueueReady,
                long maxWalletTradeSettledQueueReady,
                long maxMatchEngineQueueUnacked,
                long maxOrderTradeExecutedQueueUnacked,
                long maxWalletTradeExecutedQueueUnacked,
                long maxOrderTradeAppliedQueueUnacked,
                long maxWalletTradeSettledQueueUnacked,
                QueueDrainDiagnostics queueDrainDiagnostics) {
            return new WaitResult(
                    orderMatchedEvents,
                    orderCommandMatchedRows,
                    orderCurrentMatchedRows,
                    orderProjectionStaleRows,
                    matchHistoryRows,
                    tradeExecutions,
                    completedTrades,
                    walletTradeSettlements,
                    lockedCurrency,
                    lockedAmount,
                    buyerAvailableAmount,
                    sellerAvailableCurrency,
                    matchEngineQueueReady,
                    orderTradeExecutedQueueReady,
                    walletTradeExecutedQueueReady,
                    orderTradeAppliedQueueReady,
                    walletTradeSettledQueueReady,
                    matchEngineQueueUnacked,
                    orderTradeExecutedQueueUnacked,
                    walletTradeExecutedQueueUnacked,
                    orderTradeAppliedQueueUnacked,
                    walletTradeSettledQueueUnacked,
                    tradeExecutionsReachedSeconds,
                    orderMatchedReachedSeconds,
                    walletSettlementsReachedSeconds,
                    completedTradesReachedSeconds,
                    strictCompletedTradesReachedSeconds,
                    queueReadyDrainedSeconds,
                    queueFullyDrainedSeconds,
                    orderProjectionCaughtUpSeconds,
                    orderProjectionLagSeconds,
                    maxMatchEngineQueueReady,
                    maxOrderTradeExecutedQueueReady,
                    maxWalletTradeExecutedQueueReady,
                    maxOrderTradeAppliedQueueReady,
                    maxWalletTradeSettledQueueReady,
                    maxMatchEngineQueueUnacked,
                    maxOrderTradeExecutedQueueUnacked,
                    maxWalletTradeExecutedQueueUnacked,
                    maxOrderTradeAppliedQueueUnacked,
                    maxWalletTradeSettledQueueUnacked,
                    queueDrainDiagnostics);
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
            double minOfferedLoadRatio,
            String seedMode,
            boolean truncate,
            String redisHost,
            int redisPort,
            String rabbitHost,
            int rabbitPort,
            int publisherChannelCacheSize,
            int rabbitManagementPort,
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
                    doubleArg(args, "--min-offered-load-ratio", 0.95),
                    stringArg(args, "--seed-mode", "bulk"),
                    booleanArg(args, "--truncate", true),
                    stringArg(args, "--redis-host", "localhost"),
                    intArg(args, "--redis-port", 6379),
                    stringArg(args, "--rabbit-host", "localhost"),
                    intArg(args, "--rabbit-port", 5672),
                    intArg(args, "--publisher-channel-cache-size", Math.max(128, intArg(args, "--publishers", 16) * 2)),
                    intArg(args, "--rabbit-management-port", 15672),
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
}
