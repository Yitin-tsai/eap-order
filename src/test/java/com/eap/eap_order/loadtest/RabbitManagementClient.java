package com.eap.eap_order.loadtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class RabbitManagementClient {

    static final long UNAVAILABLE_BACKLOG = -1;

    private final String managementUrl;
    private final String vhost;
    private final String authorization;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    RabbitManagementClient(
            String managementUrl,
            String vhost,
            String user,
            String password,
            HttpClient httpClient,
            ObjectMapper objectMapper) {
        this.managementUrl = stripTrailingSlash(managementUrl);
        this.vhost = vhost;
        this.authorization = basicAuth(user, password);
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }

    QueueSnapshot readQueues(List<String> queueNames) {
        return readQueues(queueNames, false);
    }

    QueueSnapshot readQueuesAllowMissing(List<String> queueNames) {
        return readQueues(queueNames, true);
    }

    private QueueSnapshot readQueues(List<String> queueNames, boolean missingAsEmpty) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(queueCollectionUri(managementUrl, vhost))
                    .timeout(Duration.ofSeconds(3))
                    .header("Authorization", authorization)
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                return QueueSnapshot.failed(queueNames.size());
            }

            Map<String, QueueDepth> allDepths = new LinkedHashMap<>();
            for (JsonNode queue : objectMapper.readTree(response.body())) {
                allDepths.put(
                        queue.path("name").asText(),
                        new QueueDepth(
                                queue.path("messages_ready").asLong(0),
                                queue.path("messages_unacknowledged").asLong(0)));
            }

            long backlog = 0;
            long failures = 0;
            Map<String, QueueDepth> selectedDepths = new LinkedHashMap<>();
            for (String queueName : queueNames) {
                QueueDepth depth = allDepths.get(queueName);
                if (depth == null) {
                    if (missingAsEmpty) {
                        selectedDepths.put(queueName, new QueueDepth(0, 0));
                    } else {
                        failures++;
                    }
                } else {
                    selectedDepths.put(queueName, depth);
                    backlog += depth.ready() + depth.unacked();
                }
            }
            return new QueueSnapshot(backlog, failures, selectedDepths);
        } catch (Exception e) {
            System.err.printf("queue metrics read failed: %s%n", e.getMessage());
            return QueueSnapshot.failed(queueNames.size());
        }
    }

    void purgeQueues(List<String> queueNames) throws Exception {
        for (String queueName : queueNames) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(queueUri(managementUrl, vhost, queueName, "/contents"))
                    .timeout(Duration.ofSeconds(5))
                    .header("Authorization", authorization)
                    .DELETE()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (!isIdempotentPurgeStatus(response.statusCode())) {
                throw new IllegalStateException(
                        "queue purge failed: vhost=" + vhost
                                + ", queue=" + queueName
                                + ", status=" + response.statusCode());
            }
        }
    }

    static boolean isIdempotentPurgeStatus(int statusCode) {
        return (statusCode >= 200 && statusCode < 300) || statusCode == 404;
    }

    static URI queueCollectionUri(String managementUrl, String vhost) {
        return URI.create(stripTrailingSlash(managementUrl)
                + "/api/queues/" + encodePathSegment(vhost)
                + "?disable_stats=true&enable_queue_totals=true"
                + "&columns=name,messages_ready,messages_unacknowledged");
    }

    static URI queueUri(String managementUrl, String vhost, String queueName, String suffix) {
        return URI.create(stripTrailingSlash(managementUrl)
                + "/api/queues/" + encodePathSegment(vhost)
                + "/" + encodePathSegment(queueName)
                + suffix);
    }

    private static String encodePathSegment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static String basicAuth(String user, String password) {
        return "Basic " + Base64.getEncoder().encodeToString(
                (user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    record QueueDepth(long ready, long unacked) {
    }

    record QueueSnapshot(long backlog, long readFailures, Map<String, QueueDepth> depths) {
        static QueueSnapshot failed(int queueCount) {
            return new QueueSnapshot(UNAVAILABLE_BACKLOG, queueCount, Map.of());
        }
    }
}
