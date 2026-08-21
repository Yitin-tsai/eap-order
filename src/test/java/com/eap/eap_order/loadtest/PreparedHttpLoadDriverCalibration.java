package com.eap.eap_order.loadtest;

import com.sun.net.httpserver.HttpServer;

import java.io.OutputStream;
import java.lang.management.ManagementFactory;
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

public final class PreparedHttpLoadDriverCalibration {

    private PreparedHttpLoadDriverCalibration() {
    }

    public static void main(String[] args) throws Exception {
        String runId = stringArg(args, "--run-id", "DRIVER_LOCAL");
        int events = intArg(args, "--events", 20_000);
        int targetTps = intArg(args, "--target-tps", 2_000);
        int workers = intArg(args, "--workers", 128);
        int maxInFlight = intArg(args, "--max-in-flight", 512);
        if (events <= 0 || targetTps <= 0 || workers <= 0 || maxInFlight <= 0) {
            throw new IllegalArgumentException(
                    "events, target-tps, workers, and max-in-flight must be positive");
        }

        ExecutorService serverExecutor = Executors.newFixedThreadPool(4);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(serverExecutor);
        AtomicInteger received = new AtomicInteger();
        server.createContext("/orders", exchange -> {
            try (var requestBody = exchange.getRequestBody()) {
                requestBody.transferTo(OutputStream.nullOutputStream());
            }
            received.incrementAndGet();
            exchange.sendResponseHeaders(202, -1);
            exchange.close();
        });
        server.start();

        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/orders");
            long preparationStartedAtNanos = System.nanoTime();
            List<PreparedHttpLoadDriver.PreparedOrder> prepared = new ArrayList<>(events);
            for (int index = 0; index < events; index++) {
                byte[] body = ("{\"orderId\":\"" + index + "\",\"side\":\"BUY\"}")
                        .getBytes(StandardCharsets.UTF_8);
                prepared.add(new PreparedHttpLoadDriver.PreparedOrder("BUY", uri, body));
            }
            double preparationSeconds = elapsedSeconds(preparationStartedAtNanos);

            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();
            AtomicInteger accepted = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            long processCpuStartedAtNanos = processCpuTimeNanos();
            long startedAtNanos = System.nanoTime();
            long nominalTrafficNanos = Math.multiplyExact(
                    TimeUnit.SECONDS.toNanos(1), events) / targetTps;
            PreparedHttpLoadDriver.ReplayResult result = PreparedHttpLoadDriver.replay(
                    client,
                    List.copyOf(prepared),
                    targetTps,
                    workers,
                    maxInFlight,
                    startedAtNanos,
                    startedAtNanos + nominalTrafficNanos + TimeUnit.SECONDS.toNanos(30),
                    new PreparedHttpLoadDriver.ResponseRecorder() {
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
                    });
            long processCpuNanos = Math.max(0, processCpuTimeNanos() - processCpuStartedAtNanos);
            double offeredTps = result.dispatchedOrders()
                    / Math.max(result.schedulingSeconds(), 0.001);
            double offeredRatio = offeredTps / targetTps;
            boolean valid = result.dispatchedOrders() == events
                    && result.unscheduledOrders() == 0
                    && accepted.get() == events
                    && failures.get() == 0
                    && received.get() == events
                    && offeredRatio >= 0.95;

            System.out.println("{");
            System.out.println("  \"benchmarkContract\": \"prepared-http-driver-calibration\",");
            System.out.println("  \"capacityClaimAllowed\": false,");
            System.out.printf("  \"runId\": \"%s\",%n", json(runId));
            System.out.printf("  \"httpDriverMode\": \"%s\",%n", PreparedHttpLoadDriver.MODE);
            System.out.printf("  \"events\": %d,%n", events);
            System.out.printf("  \"targetTps\": %d,%n", targetTps);
            System.out.printf("  \"workers\": %d,%n", workers);
            System.out.printf("  \"maxInFlight\": %d,%n", maxInFlight);
            System.out.printf("  \"workloadPreparationSeconds\": %.4f,%n", preparationSeconds);
            System.out.printf("  \"dispatched\": %d,%n", result.dispatchedOrders());
            System.out.printf("  \"accepted\": %d,%n", accepted.get());
            System.out.printf("  \"received\": %d,%n", received.get());
            System.out.printf("  \"failures\": %d,%n", failures.get());
            System.out.printf("  \"unscheduled\": %d,%n", result.unscheduledOrders());
            System.out.printf("  \"schedulingSeconds\": %.4f,%n", result.schedulingSeconds());
            System.out.printf("  \"responseCompletionSeconds\": %.4f,%n",
                    result.responseCompletionSeconds());
            System.out.printf("  \"responseDrainTailSeconds\": %.4f,%n",
                    result.responseDrainTailSeconds());
            System.out.printf("  \"offeredTps\": %.2f,%n", offeredTps);
            System.out.printf("  \"offeredLoadRatio\": %.4f,%n", offeredRatio);
            System.out.printf("  \"processCpuSeconds\": %.4f,%n", processCpuNanos / 1_000_000_000.0);
            System.out.printf("  \"validDriverCalibration\": %s%n", valid);
            System.out.println("}");

            if (!valid) {
                throw new IllegalStateException("prepared HTTP driver calibration failed");
            }
        } finally {
            server.stop(0);
            serverExecutor.shutdownNow();
            serverExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static long processCpuTimeNanos() {
        if (ManagementFactory.getOperatingSystemMXBean()
                instanceof com.sun.management.OperatingSystemMXBean operatingSystem) {
            return operatingSystem.getProcessCpuTime();
        }
        return 0;
    }

    private static double elapsedSeconds(long startedAtNanos) {
        return (System.nanoTime() - startedAtNanos) / 1_000_000_000.0;
    }

    private static int intArg(String[] args, String name, int defaultValue) {
        return Integer.parseInt(stringArg(args, name, String.valueOf(defaultValue)));
    }

    private static String stringArg(String[] args, String name, String defaultValue) {
        for (int index = 0; index < args.length - 1; index++) {
            if (name.equals(args[index])) {
                return args[index + 1];
            }
        }
        return defaultValue;
    }

    private static String json(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
