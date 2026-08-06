package com.eap.eap_order.eventstore;

import com.eap.eap_order.configuration.publishing.OrderPublishMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.datasource.url=jdbc:postgresql://localhost:5432/eapdb",
                "spring.datasource.username=admin",
                "spring.datasource.password=admin123",
                "spring.datasource.driver-class-name=org.postgresql.Driver",
                "spring.jpa.hibernate.ddl-auto=validate",
                "spring.liquibase.enabled=true",
                "spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.xml",
                "spring.rabbitmq.listener.simple.auto-startup=false",
                "eap.scheduling.enabled=false",
                "eap.wallet.base-url=http://localhost:8081/eap-wallet",
                "eap.matchEngine.base-url=http://localhost:8082/match-engine"
        })
@EnabledIfSystemProperty(named = "eap.integration.postgres", matches = "true")
class OrderEventOutboxRelayPostgresIT {

    @Autowired
    private JdbcTemplate jdbc;

    private final List<UUID> aggregateIds = new ArrayList<>();
    private final List<OrderEventOutboxRelay> relays = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        relays.forEach(OrderEventOutboxRelay::shutdown);
        for (UUID aggregateId : aggregateIds) {
            jdbc.update("DELETE FROM order_service.order_event_outbox WHERE aggregate_id = ?", aggregateId);
        }
    }

    @Test
    void pendingBatch_shouldBeClaimedPublishedAndMarkedSent() throws Exception {
        RabbitTemplate rabbitTemplate = successfulRabbitTemplate();
        UUID first = insertOutbox("PENDING", 0, false);
        UUID second = insertOutbox("PENDING", 0, false);

        relay(rabbitTemplate).relay();

        await(Duration.ofSeconds(5), () -> status(first).equals("SENT") && status(second).equals("SENT"));
        verify(rabbitTemplate, times(2)).invoke(any(RabbitOperations.OperationsCallback.class));
    }

    @Test
    void staleInFlightBatch_shouldBeReclaimedAndMarkedSent() throws Exception {
        RabbitTemplate rabbitTemplate = successfulRabbitTemplate();
        UUID aggregateId = insertOutbox("IN_FLIGHT", 0, true);

        relay(rabbitTemplate).relay();

        await(Duration.ofSeconds(5), () -> status(aggregateId).equals("SENT"));
        verify(rabbitTemplate).invoke(any(RabbitOperations.OperationsCallback.class));
    }

    @Test
    void freshInFlightBatch_shouldNotBePublishedAgain() {
        RabbitTemplate rabbitTemplate = successfulRabbitTemplate();
        UUID aggregateId = insertOutbox("IN_FLIGHT", 0, false);

        relay(rabbitTemplate).relay();

        assertEquals("IN_FLIGHT", status(aggregateId));
        verify(rabbitTemplate, never()).invoke(any(RabbitOperations.OperationsCallback.class));
    }

    @Test
    void publishFailure_shouldReturnClaimToPendingWithRetryMetadata() throws Exception {
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        doAnswer(invocation -> {
            throw new AmqpException("broker unavailable");
        }).when(rabbitTemplate).invoke(any(RabbitOperations.OperationsCallback.class));
        UUID aggregateId = insertOutbox("PENDING", 0, false);

        relay(rabbitTemplate).relay();

        await(Duration.ofSeconds(5), () -> status(aggregateId).equals("PENDING") && attemptCount(aggregateId) == 1);
        assertTrue(jdbc.queryForObject("""
                SELECT next_retry_at > CURRENT_TIMESTAMP AND last_error LIKE '%broker unavailable%'
                FROM order_service.order_event_outbox
                WHERE aggregate_id = ?
                """, Boolean.class, aggregateId));
    }

    private OrderEventOutboxRelay relay(RabbitTemplate rabbitTemplate) {
        OrderEventOutboxRelay relay = new OrderEventOutboxRelay(
                jdbc,
                new NamedParameterJdbcTemplate(jdbc),
                rabbitTemplate,
                mock(OrderPublishMetrics.class),
                1,
                1,
                true,
                true,
                2,
                1,
                1_000);
        relays.add(relay);
        return relay;
    }

    private RabbitTemplate successfulRabbitTemplate() {
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        doAnswer(invocation -> {
            RabbitOperations.OperationsCallback<?> callback = invocation.getArgument(0);
            return callback.doInRabbit(mock(RabbitOperations.class));
        }).when(rabbitTemplate).invoke(any(RabbitOperations.OperationsCallback.class));
        return rabbitTemplate;
    }

    private UUID insertOutbox(String status, int attemptCount, boolean stale) {
        UUID aggregateId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        aggregateIds.add(aggregateId);
        jdbc.update("""
                INSERT INTO order_service.order_event_outbox
                    (event_id, aggregate_id, exchange_name, routing_key, message_type, payload,
                     status, attempt_count, next_retry_at, created_at, updated_at)
                VALUES (?, ?, 'order.exchange', 'order.submitted',
                        'com.eap.common.event.OrderSubmittedEvent', '{}'::jsonb,
                        ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP - (? * INTERVAL '1 second'))
                """, eventId, aggregateId, status, attemptCount, stale ? 10 : 0);
        return aggregateId;
    }

    private String status(UUID aggregateId) {
        return jdbc.queryForObject(
                "SELECT status FROM order_service.order_event_outbox WHERE aggregate_id = ?",
                String.class,
                aggregateId);
    }

    private int attemptCount(UUID aggregateId) {
        return jdbc.queryForObject(
                "SELECT attempt_count FROM order_service.order_event_outbox WHERE aggregate_id = ?",
                Integer.class,
                aggregateId);
    }

    private void await(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "condition did not become true within " + timeout);
    }
}
