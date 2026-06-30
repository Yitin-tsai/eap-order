package com.eap.eap_order.loadtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class OrderHttpLoadGenerator {

    private static final String DEFAULT_ORDER_URL = "http://localhost:8080/eap-order";
    private static final String DEFAULT_WALLET_URL = "http://localhost:8081/eap-wallet";

    public static void main(String[] args) throws Exception {
        Config config = Config.from(args);
        ObjectMapper objectMapper = new ObjectMapper();
        ExecutorService httpExecutor = Executors.newFixedThreadPool(config.workers());
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

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
                "sending %d HTTP orders, targetTps=%d, workers=%d%n",
                config.events(), config.tps(), config.workers());

        for (int i = 0; i < config.events(); i++) {
            int index = i;
            throttle(nextSendAtNanos, intervalNanos);
            inFlight.acquire();
            httpExecutor.execute(() -> {
                try {
                    UUID userId = users.get(index % users.size());
                    boolean buy = index % 2 == 0;
                    UUID orderId = UUID.randomUUID();
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

        System.out.println("{");
        System.out.printf("  \"events\": %d,%n", config.events());
        System.out.printf("  \"accepted\": %d,%n", accepted.get());
        System.out.printf("  \"http429\": %d,%n", tooManyRequests.get());
        System.out.printf("  \"http503\": %d,%n", unavailable.get());
        System.out.printf("  \"otherFailures\": %d,%n", otherFailures.get());
        System.out.printf("  \"elapsedSeconds\": %.2f,%n", elapsedSeconds);
        System.out.printf("  \"actualTps\": %.2f,%n", accepted.get() / Math.max(elapsedSeconds, 0.001));
        System.out.printf("  \"p50Ms\": %.2f,%n", percentileMillis(sortedLatencies, 0.50));
        System.out.printf("  \"p95Ms\": %.2f,%n", percentileMillis(sortedLatencies, 0.95));
        System.out.printf("  \"p99Ms\": %.2f%n", percentileMillis(sortedLatencies, 0.99));
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

    private record BuyRequest(UUID orderId, int bidPrice, int amount, UUID bidder) {
    }

    private record SellRequest(UUID orderId, int sellPrice, int amount, UUID seller) {
    }

    private record Config(int users, int events, int tps, int workers, String orderUrl, String walletUrl) {
        private static Config from(String[] args) {
            int tps = intArg(args, "--tps", 1000);
            int durationSeconds = intArg(args, "--duration-seconds", 0);
            int events = durationSeconds > 0
                    ? Math.multiplyExact(tps, durationSeconds)
                    : intArg(args, "--events", 10_000);
            return new Config(
                    intArg(args, "--users", 500),
                    events,
                    tps,
                    intArg(args, "--workers", 128),
                    stringArg(args, "--order-url", DEFAULT_ORDER_URL),
                    stringArg(args, "--wallet-url", DEFAULT_WALLET_URL)
            );
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
