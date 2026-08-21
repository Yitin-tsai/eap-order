package com.eap.eap_order.loadtest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

final class PreparedHttpLoadDriver {

    static final String MODE = "prepared-sync";

    private PreparedHttpLoadDriver() {
    }

    static ReplayResult replay(
            HttpClient httpClient,
            List<PreparedOrder> orders,
            int targetOrderTps,
            int workers,
            int maxInFlight,
            long firstSendAtNanos,
            long schedulingDeadlineNanos,
            ResponseRecorder recorder) throws InterruptedException {
        Objects.requireNonNull(httpClient, "httpClient");
        Objects.requireNonNull(orders, "orders");
        Objects.requireNonNull(recorder, "recorder");
        if (targetOrderTps <= 0 || workers <= 0 || maxInFlight <= 0) {
            throw new IllegalArgumentException("targetOrderTps, workers, and maxInFlight must be positive");
        }

        CountDownLatch done = new CountDownLatch(orders.size());
        Semaphore permits = new Semaphore(maxInFlight);
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / targetOrderTps;
        int dispatched = 0;
        long lastDispatchedAtNanos = firstSendAtNanos;

        try {
            for (int index = 0; index < orders.size(); index++) {
                long scheduledAtNanos = firstSendAtNanos
                        + Math.multiplyExact((long) index, intervalNanos);
                waitUntil(scheduledAtNanos);
                if (System.nanoTime() >= schedulingDeadlineNanos) {
                    break;
                }

                permits.acquire();
                if (System.nanoTime() >= schedulingDeadlineNanos) {
                    permits.release();
                    break;
                }

                PreparedOrder order = orders.get(index);
                long requestStartedAtNanos = System.nanoTime();
                executor.execute(() -> {
                    try {
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(order.uri())
                                .timeout(Duration.ofSeconds(10))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofByteArray(order.body()))
                                .build();
                        HttpResponse<Void> response = httpClient.send(
                                request, HttpResponse.BodyHandlers.discarding());
                        recorder.recordResponse(
                                order.side(),
                                response.statusCode(),
                                System.nanoTime() - requestStartedAtNanos);
                    } catch (Exception failure) {
                        recorder.recordFailure(order.side(), failure);
                    } finally {
                        permits.release();
                        done.countDown();
                    }
                });
                dispatched++;
                lastDispatchedAtNanos = requestStartedAtNanos;
            }

            int unscheduled = orders.size() - dispatched;
            for (int index = 0; index < unscheduled; index++) {
                done.countDown();
            }
            done.await();
            long responsesCompletedAtNanos = System.nanoTime();
            double schedulingSeconds = dispatched == 0
                    ? 0
                    : (lastDispatchedAtNanos - firstSendAtNanos + intervalNanos)
                            / 1_000_000_000.0;
            double responseCompletionSeconds =
                    (responsesCompletedAtNanos - firstSendAtNanos) / 1_000_000_000.0;
            double responseDrainTailSeconds = dispatched == 0
                    ? 0
                    : Math.max(0,
                            (responsesCompletedAtNanos - lastDispatchedAtNanos)
                                    / 1_000_000_000.0);
            return new ReplayResult(
                    dispatched,
                    unscheduled,
                    schedulingSeconds,
                    responseCompletionSeconds,
                    responseDrainTailSeconds);
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    private static void waitUntil(long targetNanos) throws InterruptedException {
        long remainingNanos = targetNanos - System.nanoTime();
        if (remainingNanos > 0) {
            TimeUnit.NANOSECONDS.sleep(remainingNanos);
        }
    }

    record PreparedOrder(String side, URI uri, byte[] body) {
        PreparedOrder {
            Objects.requireNonNull(side, "side");
            Objects.requireNonNull(uri, "uri");
            Objects.requireNonNull(body, "body");
        }
    }

    interface ResponseRecorder {
        void recordResponse(String side, int statusCode, long latencyNanos);

        void recordFailure(String side, Throwable failure);
    }

    record ReplayResult(
            int dispatchedOrders,
            int unscheduledOrders,
            double schedulingSeconds,
            double responseCompletionSeconds,
            double responseDrainTailSeconds) {
    }
}
