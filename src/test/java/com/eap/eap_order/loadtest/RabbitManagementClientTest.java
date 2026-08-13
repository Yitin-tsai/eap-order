package com.eap.eap_order.loadtest;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitManagementClientTest {

    @Test
    void queueCollectionUri_scopesMetricsToEncodedVhost() {
        assertEquals(
                "http://rabbit.example/api/queues/team%2Fload"
                        + "?disable_stats=true&enable_queue_totals=true"
                        + "&columns=name,messages_ready,messages_unacknowledged",
                RabbitManagementClient.queueCollectionUri(
                        "http://rabbit.example/", "team/load").toString());
    }

    @Test
    void queueUri_encodesDefaultVhostAndQueueName() {
        assertEquals(
                "http://rabbit.example/api/queues/%2F/wallet.trade%20queue/contents",
                RabbitManagementClient.queueUri(
                        "http://rabbit.example", "/", "wallet.trade queue", "/contents").toString());
    }

    @Test
    void failedSnapshot_marksBacklogUnavailableWithoutOverflowSentinel() {
        var snapshot = RabbitManagementClient.QueueSnapshot.failed(7);

        assertEquals(RabbitManagementClient.UNAVAILABLE_BACKLOG, snapshot.backlog());
        assertEquals(7, snapshot.readFailures());
        assertTrue(snapshot.depths().isEmpty());
    }
}
