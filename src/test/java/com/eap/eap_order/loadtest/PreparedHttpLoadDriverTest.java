package com.eap.eap_order.loadtest;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class PreparedHttpLoadDriverTest {

    @Test
    void replay_shouldDispatchPreparedBodiesAndCollectResponses() throws Exception {
        ExecutorService serverExecutor = Executors.newFixedThreadPool(2);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(serverExecutor);
        AtomicInteger received = new AtomicInteger();
        server.createContext("/orders", exchange -> {
            try (var body = exchange.getRequestBody()) {
                body.transferTo(OutputStream.nullOutputStream());
            }
            received.incrementAndGet();
            exchange.sendResponseHeaders(202, -1);
            exchange.close();
        });
        server.start();

        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/orders");
            List<PreparedHttpLoadDriver.PreparedOrder> orders = new ArrayList<>();
            for (int index = 0; index < 200; index++) {
                orders.add(new PreparedHttpLoadDriver.PreparedOrder(
                        index % 2 == 0 ? "BUY" : "SELL",
                        uri,
                        ("{\"index\":" + index + "}").getBytes(StandardCharsets.UTF_8)));
            }
            AtomicInteger accepted = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            long startedAtNanos = System.nanoTime();

            var result = PreparedHttpLoadDriver.replay(
                    HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build(),
                    List.copyOf(orders),
                    1_000,
                    16,
                    64,
                    startedAtNanos,
                    startedAtNanos + TimeUnit.SECONDS.toNanos(5),
                    recorder(accepted, failures));

            assertThat(result.dispatchedOrders()).isEqualTo(200);
            assertThat(result.unscheduledOrders()).isZero();
            assertThat(accepted).hasValue(200);
            assertThat(failures).hasValue(0);
            assertThat(received).hasValue(200);
            assertThat(result.schedulingSeconds()).isBetween(0.15, 1.0);
            assertThat(result.responseCompletionSeconds()).isGreaterThan(0.15);
            assertThat(result.responseDrainTailSeconds()).isGreaterThanOrEqualTo(0);
        } finally {
            server.stop(0);
            serverExecutor.shutdownNow();
            serverExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void replay_whenDeadlineAlreadyPassed_shouldLeaveAllOrdersUnscheduled() throws Exception {
        URI uri = URI.create("http://127.0.0.1:1/orders");
        List<PreparedHttpLoadDriver.PreparedOrder> orders = List.of(
                new PreparedHttpLoadDriver.PreparedOrder("BUY", uri, new byte[]{1}),
                new PreparedHttpLoadDriver.PreparedOrder("SELL", uri, new byte[]{2}));
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        long startedAtNanos = System.nanoTime();

        var result = PreparedHttpLoadDriver.replay(
                HttpClient.newHttpClient(),
                orders,
                1_000,
                2,
                4,
                startedAtNanos,
                startedAtNanos - 1,
                recorder(accepted, failures));

        assertThat(result.dispatchedOrders()).isZero();
        assertThat(result.unscheduledOrders()).isEqualTo(2);
        assertThat(accepted).hasValue(0);
        assertThat(failures).hasValue(0);
    }

    private static PreparedHttpLoadDriver.ResponseRecorder recorder(
            AtomicInteger accepted,
            AtomicInteger failures) {
        return new PreparedHttpLoadDriver.ResponseRecorder() {
            @Override
            public void recordResponse(String side, int statusCode, long latencyNanos) {
                if (statusCode == 202) {
                    accepted.incrementAndGet();
                } else {
                    failures.incrementAndGet();
                }
            }

            @Override
            public void recordFailure(String side, Throwable failure) {
                failures.incrementAndGet();
            }
        };
    }
}
