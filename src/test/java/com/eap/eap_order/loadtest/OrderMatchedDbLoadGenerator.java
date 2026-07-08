package com.eap.eap_order.loadtest;

import com.eap.common.event.OrderConfirmedEvent;
import com.eap.common.event.OrderMatchedEvent;
import com.eap.common.event.OrderSubmittedEvent;
import com.eap.eap_order.EapOrderApplication;
import com.eap.eap_order.application.OrderEventSourcingService;
import com.eap.eap_order.configuration.repository.MathedOrderRepository;
import com.eap.eap_order.domain.entity.MatchOrderEntity;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

public class OrderMatchedDbLoadGenerator {

    private static final String MARKET_ID = "ORDER_MATCHED_DB_LOAD";

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
            MathedOrderRepository matchOrderRepository = context.getBean(MathedOrderRepository.class);

            if (config.truncate()) {
                truncateOrderTestData(jdbcTemplate);
            }

            System.out.printf(
                    "seeding %d valid matched pairs (%d order streams)%n",
                    config.events(), config.events() * 2);
            List<Pair> pairs = seedOpenOrders(config, eventSourcingService);

            System.out.printf(
                    "matching %d pairs, workers=%d%n",
                    config.events(), config.workers());
            Result result = runMatchedDbLoad(config, pairs, eventSourcingService, matchOrderRepository);

            long matchedEvents = count(jdbcTemplate, """
                    SELECT count(*)
                    FROM order_service.order_event_store
                    WHERE event_type = 'OrderMatchedV1'
                    """);
            long matchHistoryRows = count(jdbcTemplate, "SELECT count(*) FROM order_service.match_history");
            long duplicateAggregateVersions = count(jdbcTemplate, """
                    SELECT count(*)
                    FROM (
                        SELECT aggregate_id, aggregate_version
                        FROM order_service.order_event_store
                        GROUP BY aggregate_id, aggregate_version
                        HAVING count(*) > 1
                    ) d
                    """);
            long brokenHashLinks = count(jdbcTemplate, """
                    SELECT count(*)
                    FROM order_service.order_event_store child
                    JOIN order_service.order_event_store parent
                      ON parent.aggregate_id = child.aggregate_id
                     AND parent.aggregate_version = child.aggregate_version - 1
                    WHERE child.prev_hash IS DISTINCT FROM parent.hash
                    """);

            printResult(config, result, matchedEvents, matchHistoryRows, duplicateAggregateVersions, brokenHashLinks);

            require(result.failures() == 0, "matched DB load should have no failures");
            require(matchedEvents == config.events() * 2L, "each match should append buyer and seller OrderMatchedV1");
            require(matchHistoryRows == config.events(), "each match should create one match history row");
            require(duplicateAggregateVersions == 0, "aggregate versions should be unique");
            require(brokenHashLinks == 0, "event hash chain should remain linked");
        }
    }

    private static List<Pair> seedOpenOrders(Config config, OrderEventSourcingService eventSourcingService) {
        List<Pair> pairs = new ArrayList<>(config.events());
        for (int i = 0; i < config.events(); i++) {
            UUID buyerId = UUID.randomUUID();
            UUID sellerId = UUID.randomUUID();
            UUID buyOrderId = UUID.randomUUID();
            UUID sellOrderId = UUID.randomUUID();
            long sequence = (long) i * 2L + 1L;

            OrderSubmittedEvent buySubmitted = submitted(buyOrderId, buyerId, "BUY", sequence);
            OrderSubmittedEvent sellSubmitted = submitted(sellOrderId, sellerId, "SELL", sequence + 1L);
            eventSourcingService.request(buySubmitted);
            eventSourcingService.confirm(confirmed(buySubmitted));
            eventSourcingService.request(sellSubmitted);
            eventSourcingService.confirm(confirmed(sellSubmitted));

            pairs.add(new Pair(i + 1, buyOrderId, sellOrderId, buyerId, sellerId, sequence, sequence + 1L));
        }
        return pairs;
    }

    private static Result runMatchedDbLoad(
            Config config,
            List<Pair> pairs,
            OrderEventSourcingService eventSourcingService,
            MathedOrderRepository matchOrderRepository) throws InterruptedException {
        AtomicInteger processed = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(pairs.size());
        ExecutorService executor = Executors.newFixedThreadPool(config.workers());
        Semaphore inFlight = new Semaphore(config.workers() * 2);
        List<Long> latenciesNanos = Collections.synchronizedList(new ArrayList<>(pairs.size()));

        long started = System.nanoTime();
        for (Pair pair : pairs) {
            inFlight.acquire();
            executor.execute(() -> {
                try {
                    long itemStarted = System.nanoTime();
                    OrderMatchedEvent event = matched(pair);
                    if (!matchOrderRepository.existsByMatchId(event.getMatchId())) {
                        matchOrderRepository.save(MatchOrderEntity.builder()
                                .matchId(event.getMatchId())
                                .buyerUuid(event.getBuyerId())
                                .sellerUuid(event.getSellerId())
                                .price(event.getDealPrice())
                                .amount(event.getAmount())
                                .updateTime(event.getMatchedAt())
                                .orderType(event.getOrderType())
                                .build());
                    }
                    eventSourcingService.match(event.getBuyerOrderId(), event);
                    eventSourcingService.match(event.getSellerOrderId(), event);
                    latenciesNanos.add(System.nanoTime() - itemStarted);
                    processed.incrementAndGet();
                } catch (Exception e) {
                    failures.incrementAndGet();
                    if (failures.get() <= 10) {
                        System.err.printf("matched db write failed: matchId=%d, error=%s%n",
                                pair.matchId(), e.getMessage());
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
        List<Long> sortedLatencies = new ArrayList<>(latenciesNanos);
        Collections.sort(sortedLatencies);
        return new Result(
                processed.get(),
                failures.get(),
                elapsedSeconds,
                percentileMillis(sortedLatencies, 0.50),
                percentileMillis(sortedLatencies, 0.95),
                percentileMillis(sortedLatencies, 0.99));
    }

    private static OrderSubmittedEvent submitted(UUID orderId, UUID userId, String side, long sequence) {
        return OrderSubmittedEvent.builder()
                .orderId(orderId)
                .userId(userId)
                .marketId(MARKET_ID)
                .marketSequence(sequence)
                .price(100)
                .amount(1)
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
                .createdAt(LocalDateTime.now())
                .build();
    }

    private static OrderMatchedEvent matched(Pair pair) {
        return OrderMatchedEvent.builder()
                .matchId(pair.matchId())
                .buyerId(pair.buyerId())
                .sellerId(pair.sellerId())
                .buyerOrderId(pair.buyOrderId())
                .sellerOrderId(pair.sellOrderId())
                .marketId(MARKET_ID)
                .buyerMarketSequence(pair.buySequence())
                .sellerMarketSequence(pair.sellSequence())
                .originBuyerPrice(100)
                .originSellerPrice(100)
                .dealPrice(100)
                .amount(1)
                .matchedAt(LocalDateTime.now())
                .orderType("BUY")
                .build();
    }

    private static void truncateOrderTestData(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute("""
                TRUNCATE TABLE
                    order_service.match_history,
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

    private static long count(JdbcTemplate jdbcTemplate, String sql) {
        Long value = jdbcTemplate.queryForObject(sql, Long.class);
        return value == null ? 0L : value;
    }

    private static double percentileMillis(List<Long> sorted, double percentile) {
        if (sorted.isEmpty()) {
            return 0;
        }
        int index = (int) Math.ceil(percentile * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1))) / 1_000_000.0;
    }

    private static void printResult(
            Config config,
            Result result,
            long matchedEvents,
            long matchHistoryRows,
            long duplicateAggregateVersions,
            long brokenHashLinks) {
        System.out.println("{");
        System.out.printf("  \"mode\": \"orderMatchedDb\",%n");
        System.out.printf("  \"matches\": %d,%n", config.events());
        System.out.printf("  \"workers\": %d,%n", config.workers());
        System.out.printf("  \"processed\": %d,%n", result.processed());
        System.out.printf("  \"failures\": %d,%n", result.failures());
        System.out.printf("  \"elapsedSeconds\": %.2f,%n", result.elapsedSeconds());
        System.out.printf("  \"matchedDbTps\": %.2f,%n", result.processed() / Math.max(result.elapsedSeconds(), 0.001));
        System.out.printf("  \"p50Ms\": %.2f,%n", result.p50Ms());
        System.out.printf("  \"p95Ms\": %.2f,%n", result.p95Ms());
        System.out.printf("  \"p99Ms\": %.2f,%n", result.p99Ms());
        System.out.printf("  \"orderMatchedEvents\": %d,%n", matchedEvents);
        System.out.printf("  \"matchHistoryRows\": %d,%n", matchHistoryRows);
        System.out.printf("  \"duplicateAggregateVersions\": %d,%n", duplicateAggregateVersions);
        System.out.printf("  \"brokenHashLinks\": %d%n", brokenHashLinks);
        System.out.println("}");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private record Pair(
            int matchId,
            UUID buyOrderId,
            UUID sellOrderId,
            UUID buyerId,
            UUID sellerId,
            long buySequence,
            long sellSequence) {
    }

    private record Result(
            int processed,
            int failures,
            double elapsedSeconds,
            double p50Ms,
            double p95Ms,
            double p99Ms) {
    }

    private record Config(int events, int workers, boolean truncate) {
        private static Config from(String[] args) {
            return new Config(
                    intArg(args, "--events", 1_000),
                    intArg(args, "--workers", 16),
                    booleanArg(args, "--truncate", true));
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
