package com.eap.eap_order.loadtest;

import com.eap.common.event.OrderConfirmedEvent;
import com.eap.common.event.OrderSubmittedEvent;
import com.eap.eap_order.EapOrderApplication;
import com.eap.eap_order.application.OrderEventSourcingService;
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

import static com.eap.common.constants.RabbitMQConstants.MATCH_ENGINE_ORDER_CONFIRMED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_MATCHED_QUEUE;
import static com.eap.common.constants.RabbitMQConstants.WALLET_ORDER_MATCHED_QUEUE;

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
                        "spring.datasource.url=jdbc:postgresql://localhost:5432/eapdb",
                        "spring.datasource.username=admin",
                        "spring.datasource.password=admin123",
                        "spring.datasource.driver-class-name=org.postgresql.Driver",
                        "spring.jpa.hibernate.ddl-auto=validate",
                        "spring.liquibase.enabled=true",
                        "spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.xml",
                        "eap.scheduling.enabled=false",
                        "eap.order-event-outbox.batch-size=0",
                        "eap.order-projection.enabled=false",
                        "eap.matchEngine.base-url=http://localhost:8082/match-engine",
                        "eap.wallet.base-url=http://localhost:8081/eap-wallet",
                        "logging.level.com.eap.eap_order=WARN",
                        "logging.level.org.springframework.amqp=WARN",
                        "logging.level.org.hibernate=WARN")
                .run()) {

            JdbcTemplate jdbcTemplate = context.getBean(JdbcTemplate.class);
            OrderEventSourcingService eventSourcingService = context.getBean(OrderEventSourcingService.class);
            ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

            CachingConnectionFactory rabbitConnectionFactory = rabbitConnectionFactory(config);
            RabbitTemplate rabbitTemplate = rabbitTemplate(rabbitConnectionFactory, objectMapper);
            RabbitAdmin rabbitAdmin = new RabbitAdmin(rabbitConnectionFactory);
            LettuceConnectionFactory redisConnectionFactory = redisConnectionFactory(config);
            RedisTemplate<String, String> redisTemplate = redisTemplate(redisConnectionFactory);

            try {
                List<Pair> pairs = buildPairs(config);
                switch (config.phase()) {
                    case "seed" -> seed(config, pairs, jdbcTemplate, eventSourcingService, rabbitAdmin, redisTemplate);
                    case "run" -> run(config, pairs, jdbcTemplate, rabbitTemplate, rabbitAdmin, redisTemplate);
                    case "all" -> {
                        seed(config, pairs, jdbcTemplate, eventSourcingService, rabbitAdmin, redisTemplate);
                        run(config, pairs, jdbcTemplate, rabbitTemplate, rabbitAdmin, redisTemplate);
                    }
                    default -> throw new IllegalArgumentException("--phase must be seed, run, or all");
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
            JdbcTemplate jdbcTemplate,
            OrderEventSourcingService eventSourcingService,
            RabbitAdmin rabbitAdmin,
            RedisTemplate<String, String> redisTemplate) {
        if (config.truncate()) {
            truncateOrderAndWalletTestData(jdbcTemplate);
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

        jdbcTemplate.execute("TRUNCATE TABLE order_service.order_event_outbox RESTART IDENTITY");
        seedWallets(jdbcTemplate, pairs);
        System.out.println("seed complete; start matchEngine/order/wallet services before --phase run");
    }

    private static void run(
            Config config,
            List<Pair> pairs,
            JdbcTemplate jdbcTemplate,
            RabbitTemplate rabbitTemplate,
            RabbitAdmin rabbitAdmin,
            RedisTemplate<String, String> redisTemplate) throws Exception {
        purgeQueues(rabbitAdmin);
        cleanupRedis(config, pairs, redisTemplate);

        System.out.printf("publishing %d resting SELL confirmations directly to %s%n",
                config.events(), MATCH_ENGINE_ORDER_CONFIRMED_QUEUE);
        PublishResult sellPublish = publishToMatchEngine(config, rabbitTemplate, pairs, true);
        waitForRedisSellBook(config, redisTemplate);

        System.out.printf("publishing %d incoming BUY confirmations directly to %s%n",
                config.events(), MATCH_ENGINE_ORDER_CONFIRMED_QUEUE);
        long started = System.nanoTime();
        PublishResult buyPublish = publishToMatchEngine(config, rabbitTemplate, pairs, false);
        WaitResult waitResult = waitForDownstream(config, jdbcTemplate, rabbitAdmin);
        double elapsedSeconds = (System.nanoTime() - started) / 1_000_000_000.0;

        long remainingSellOrders = zsetSize(redisTemplate, "orderbook:" + config.marketId() + ":sell");
        long remainingBuyOrders = zsetSize(redisTemplate, "orderbook:" + config.marketId() + ":buy");
        printResult(config, sellPublish, buyPublish, waitResult, elapsedSeconds, remainingSellOrders, remainingBuyOrders);

        require(sellPublish.failures() == 0, "SELL publish should have no failures");
        require(buyPublish.failures() == 0, "BUY publish should have no failures");
        require(waitResult.orderMatchedEvents() == config.events() * 2L, "Order should append buyer and seller matched events");
        require(waitResult.matchHistoryRows() == config.events(), "Order should save one match_history row per match");
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
        AtomicInteger published = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(pairs.size());
        ExecutorService executor = Executors.newFixedThreadPool(config.publishers());
        Semaphore inFlight = new Semaphore(config.publishers() * 2);

        long started = System.nanoTime();
        for (Pair pair : pairs) {
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
        return new PublishResult(published.get(), failures.get(), elapsedSeconds);
    }

    private static void waitForRedisSellBook(Config config, RedisTemplate<String, String> redisTemplate) throws InterruptedException {
        String key = "orderbook:" + config.marketId() + ":sell";
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.timeoutSeconds());
        while (System.nanoTime() < deadline) {
            if (zsetSize(redisTemplate, key) == config.events()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        throw new IllegalStateException("Timed out waiting for resting SELL orders in Redis order book");
    }

    private static WaitResult waitForDownstream(Config config, JdbcTemplate jdbcTemplate, RabbitAdmin rabbitAdmin)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.timeoutSeconds());
        WaitResult latest;
        do {
            latest = snapshot(jdbcTemplate, rabbitAdmin);
            if (latest.orderMatchedEvents() == config.events() * 2L
                    && latest.matchHistoryRows() == config.events()
                    && latest.lockedCurrency() == 0
                    && latest.lockedAmount() == 0
                    && latest.buyerAvailableAmount() == config.events() * (long) AMOUNT
                    && latest.sellerAvailableCurrency() == config.events() * (long) PRICE * AMOUNT
                    && latest.matchEngineQueueReady() == 0
                    && latest.orderMatchedQueueReady() == 0
                    && latest.walletMatchedQueueReady() == 0) {
                return latest;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        } while (System.nanoTime() < deadline);
        return latest;
    }

    private static WaitResult snapshot(JdbcTemplate jdbcTemplate, RabbitAdmin rabbitAdmin) {
        return new WaitResult(
                count(jdbcTemplate, "SELECT count(*) FROM order_service.order_event_store WHERE event_type = 'OrderMatchedV1'"),
                count(jdbcTemplate, "SELECT count(*) FROM order_service.match_history"),
                count(jdbcTemplate, "SELECT COALESCE(sum(locked_currency), 0) FROM wallet_service.wallets"),
                count(jdbcTemplate, "SELECT COALESCE(sum(locked_amount), 0) FROM wallet_service.wallets"),
                count(jdbcTemplate, "SELECT COALESCE(sum(available_amount), 0) FROM wallet_service.wallets"),
                count(jdbcTemplate, "SELECT COALESCE(sum(available_currency), 0) FROM wallet_service.wallets"),
                queueReady(rabbitAdmin, MATCH_ENGINE_ORDER_CONFIRMED_QUEUE),
                queueReady(rabbitAdmin, ORDER_ORDER_MATCHED_QUEUE),
                queueReady(rabbitAdmin, WALLET_ORDER_MATCHED_QUEUE));
    }

    private static void truncateOrderAndWalletTestData(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute("""
                TRUNCATE TABLE
                    order_service.match_history,
                    order_service.order_event_outbox,
                    order_service.orders_current,
                    order_service.projection_checkpoints,
                    order_service.order_event_store,
                    order_service.order_stream_heads
                RESTART IDENTITY CASCADE
                """);
        jdbcTemplate.execute("""
                TRUNCATE TABLE
                    wallet_service.outbox,
                    wallet_service.order_submission_idempotency,
                    wallet_service.settlement_idempotency,
                    wallet_service.wallets
                RESTART IDENTITY CASCADE
                """);
    }

    private static void purgeQueues(RabbitAdmin rabbitAdmin) {
        purgeIfPresent(rabbitAdmin, MATCH_ENGINE_ORDER_CONFIRMED_QUEUE);
        purgeIfPresent(rabbitAdmin, ORDER_ORDER_MATCHED_QUEUE);
        purgeIfPresent(rabbitAdmin, WALLET_ORDER_MATCHED_QUEUE);
    }

    private static void purgeIfPresent(RabbitAdmin rabbitAdmin, String queueName) {
        try {
            rabbitAdmin.purgeQueue(queueName, true);
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
        System.out.printf("  \"sellPublished\": %d,%n", sellPublish.published());
        System.out.printf("  \"sellPublishFailures\": %d,%n", sellPublish.failures());
        System.out.printf("  \"buyPublished\": %d,%n", buyPublish.published());
        System.out.printf("  \"buyPublishFailures\": %d,%n", buyPublish.failures());
        System.out.printf("  \"elapsedSeconds\": %.2f,%n", elapsedSeconds);
        System.out.printf("  \"matchedE2eTps\": %.2f,%n", config.events() / Math.max(elapsedSeconds, 0.001));
        System.out.printf("  \"orderMatchedEvents\": %d,%n", waitResult.orderMatchedEvents());
        System.out.printf("  \"matchHistoryRows\": %d,%n", waitResult.matchHistoryRows());
        System.out.printf("  \"lockedCurrency\": %d,%n", waitResult.lockedCurrency());
        System.out.printf("  \"lockedAmount\": %d,%n", waitResult.lockedAmount());
        System.out.printf("  \"buyerAvailableAmount\": %d,%n", waitResult.buyerAvailableAmount());
        System.out.printf("  \"sellerAvailableCurrency\": %d,%n", waitResult.sellerAvailableCurrency());
        System.out.printf("  \"matchEngineQueueReady\": %d,%n", waitResult.matchEngineQueueReady());
        System.out.printf("  \"orderMatchedQueueReady\": %d,%n", waitResult.orderMatchedQueueReady());
        System.out.printf("  \"walletMatchedQueueReady\": %d,%n", waitResult.walletMatchedQueueReady());
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

    private record PublishResult(int published, int failures, double elapsedSeconds) {
    }

    private record WaitResult(
            long orderMatchedEvents,
            long matchHistoryRows,
            long lockedCurrency,
            long lockedAmount,
            long buyerAvailableAmount,
            long sellerAvailableCurrency,
            long matchEngineQueueReady,
            long orderMatchedQueueReady,
            long walletMatchedQueueReady) {
    }

    private record Config(
            String phase,
            String marketId,
            int events,
            int publishers,
            int timeoutSeconds,
            boolean truncate,
            String redisHost,
            int redisPort,
            String rabbitHost,
            int rabbitPort,
            String rabbitUser,
            String rabbitPassword) {

        private static Config from(String[] args) {
            String marketId = stringArg(args, "--market-id", "MATCHED_E2E_LOAD");
            return new Config(
                    stringArg(args, "--phase", "run"),
                    marketId,
                    intArg(args, "--events", 1_000),
                    intArg(args, "--publishers", 16),
                    intArg(args, "--timeout-seconds", 60),
                    booleanArg(args, "--truncate", true),
                    stringArg(args, "--redis-host", "localhost"),
                    intArg(args, "--redis-port", 6379),
                    stringArg(args, "--rabbit-host", "localhost"),
                    intArg(args, "--rabbit-port", 5672),
                    stringArg(args, "--rabbit-user", "admin"),
                    stringArg(args, "--rabbit-pass", "admin123"));
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
