package com.eap.eap_order.application;

import com.eap.common.event.OrderCancellationResultEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.datasource.url=jdbc:postgresql://localhost:5432/eapdb",
                "spring.datasource.username=admin",
                "spring.datasource.password=admin123",
                "spring.jpa.hibernate.ddl-auto=validate",
                "spring.liquibase.enabled=true",
                "spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.xml",
                "spring.rabbitmq.listener.simple.auto-startup=false",
                "eap.scheduling.enabled=false",
                "eap.wallet.base-url=http://localhost:8081/eap-wallet",
                "eap.matchEngine.base-url=http://localhost:8082/match-engine",
                "eap.order.cancellation-result-reconciler.initial-delay-ms=3600000",
                "eap.order.cancellation-result-reconciler.poll-interval-ms=3600000"
        })
@EnabledIfSystemProperty(named = "eap.integration.postgres", matches = "true")
class OrderCancellationResultInboxPostgresIT {

    @Autowired
    private OrderCancellationResultInbox inbox;
    @Autowired
    private JdbcTemplate jdbc;

    private final List<UUID> cancellationIds = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        for (UUID cancellationId : cancellationIds) {
            jdbc.update("""
                    DELETE FROM order_service.order_cancellation_result_inbox
                    WHERE cancellation_id = ?
                    """, cancellationId);
        }
    }

    @Test
    void receive_shouldTreatDifferentJsonPropertyOrderAsTheSameEvent() {
        OrderCancellationResultEvent event = event(4);
        jdbc.update("""
                INSERT INTO order_service.order_cancellation_result_inbox
                    (cancellation_id, order_id, payload)
                VALUES (?, ?, ?)
                """, event.getCancellationId(), event.getOrderId(), """
                {
                  "decidedAt":"2026-08-24T12:00:00",
                  "cancelledAmount":4,
                  "limitPrice":120,
                  "orderType":"BUY",
                  "reason":null,
                  "outcome":"CANCELLED",
                  "userId":"%s",
                  "orderId":"%s",
                  "cancellationId":"%s"
                }
                """.formatted(event.getUserId(), event.getOrderId(), event.getCancellationId()));

        assertThatCode(() -> inbox.receive(event)).doesNotThrowAnyException();
    }

    @Test
    void receive_shouldRejectAConflictingCancelledRemainder() {
        OrderCancellationResultEvent persisted = event(4);
        inbox.receive(persisted);
        OrderCancellationResultEvent conflicting = event(3);
        conflicting.setCancellationId(persisted.getCancellationId());
        conflicting.setOrderId(persisted.getOrderId());
        conflicting.setUserId(persisted.getUserId());

        assertThatThrownBy(() -> inbox.receive(conflicting))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("identity conflict");
    }

    private OrderCancellationResultEvent event(int cancelledAmount) {
        UUID cancellationId = UUID.randomUUID();
        cancellationIds.add(cancellationId);
        return OrderCancellationResultEvent.builder()
                .cancellationId(cancellationId)
                .orderId(UUID.randomUUID())
                .userId(UUID.randomUUID())
                .outcome(OrderCancellationResultEvent.CANCELLED)
                .orderType("BUY")
                .limitPrice(120)
                .cancelledAmount(cancelledAmount)
                .decidedAt(LocalDateTime.of(2026, 8, 24, 12, 0))
                .build();
    }
}
