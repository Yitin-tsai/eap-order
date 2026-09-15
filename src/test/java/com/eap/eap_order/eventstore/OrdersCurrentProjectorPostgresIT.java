package com.eap.eap_order.eventstore;

import com.eap.eap_order.configuration.observability.OrderDurableDebtSnapshotProvider;
import com.eap.eap_order.domain.ordersourcing.OrderSubmissionRequestedV1;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                "eap.order-projection.enabled=false",
                "eap.wallet.base-url=http://localhost:8081/eap-wallet",
                "eap.matchEngine.base-url=http://localhost:8082/match-engine"
        })
@EnabledIfSystemProperty(named = "eap.integration.postgres", matches = "true")
class OrdersCurrentProjectorPostgresIT {

    @Autowired
    private OrderEventAppender appender;

    @Autowired
    private OrdersCurrentProjector projector;

    @Autowired
    private OrderDurableDebtSnapshotProvider durableDebt;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void projectionFailure_shouldRemainDurableUntilThePoisonEventIsRepairedAndApplied()
            throws Exception {
        projector.projectUntilCaughtUpIgnoringEnabled();
        long originalCheckpoint = checkpoint();
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        try {
            OrderEventAppendResult appended = appender.append(new OrderEventAppendCommand(
                    orderId,
                    0,
                    eventId,
                    "UnsupportedProjectionEventV1",
                    Map.of("orderId", orderId),
                    Map.of("test", "projection-failure"),
                    1,
                    LocalDateTime.now(),
                    null));

            assertThatThrownBy(projector::projectUntilCaughtUpIgnoringEnabled)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Projection failed at globalPosition=");

            assertThat(checkpoint()).isEqualTo(originalCheckpoint);
            assertThat(failureCount()).isEqualTo(1);
            assertThat(jdbc.queryForObject("""
                    SELECT first_failure_at IS NOT NULL
                           AND last_failure_at IS NOT NULL
                           AND last_error LIKE '%Unsupported projection event%'
                    FROM order_service.projection_checkpoints
                    WHERE projection_name = 'orders_current'
                    """, Boolean.class)).isTrue();

            ReflectionTestUtils.invokeMethod(durableDebt, "refresh");
            var failedProjection = durableDebt.snapshot().components().stream()
                    .filter(component -> component.work().equals("orders_current_projection"))
                    .findFirst()
                    .orElseThrow();
            assertThat(failedProjection.totalCount()).isGreaterThanOrEqualTo(1);
            assertThat(failedProjection.retryCount()).isEqualTo(1);

            OrderSubmissionRequestedV1 repaired = new OrderSubmissionRequestedV1(
                    orderId,
                    UUID.randomUUID(),
                    "ENERGY-SPOT",
                    99_999L,
                    "BUY",
                    100,
                    1,
                    LocalDateTime.now());
            String repairedPayload = objectMapper.writeValueAsString(repaired);
            jdbc.update("""
                    UPDATE order_service.order_event_store
                    SET event_type = 'OrderSubmissionRequestedV1',
                        payload_canonical = ?
                    WHERE event_id = ?
                    """, repairedPayload, eventId);

            projector.projectUntilCaughtUpIgnoringEnabled();

            assertThat(checkpoint()).isEqualTo(appended.globalPosition());
            assertThat(failureCount()).isZero();
            assertThat(jdbc.queryForObject("""
                    SELECT first_failure_at IS NULL
                           AND last_failure_at IS NULL
                           AND last_error IS NULL
                    FROM order_service.projection_checkpoints
                    WHERE projection_name = 'orders_current'
                    """, Boolean.class)).isTrue();
            assertThat(jdbc.queryForObject(
                    "SELECT count(*) FROM order_service.orders_current WHERE order_id = ?",
                    Long.class,
                    orderId)).isEqualTo(1);
        } finally {
            jdbc.update("DELETE FROM order_service.orders_current WHERE order_id = ?", orderId);
            jdbc.update("DELETE FROM order_service.order_event_outbox WHERE event_id = ?", eventId);
            jdbc.update("DELETE FROM order_service.order_event_store WHERE event_id = ?", eventId);
            jdbc.update("DELETE FROM order_service.order_stream_heads WHERE aggregate_id = ?", orderId);
            jdbc.update("""
                    UPDATE order_service.projection_checkpoints
                    SET last_global_position = ?, failure_count = 0,
                        first_failure_at = NULL, last_failure_at = NULL, last_error = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE projection_name = 'orders_current'
                    """, originalCheckpoint);
        }
    }

    private long checkpoint() {
        return jdbc.queryForObject("""
                SELECT last_global_position
                FROM order_service.projection_checkpoints
                WHERE projection_name = 'orders_current'
                """, Long.class);
    }

    private int failureCount() {
        return jdbc.queryForObject("""
                SELECT failure_count
                FROM order_service.projection_checkpoints
                WHERE projection_name = 'orders_current'
                """, Integer.class);
    }
}
