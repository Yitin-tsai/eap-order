package com.eap.eap_order.application;

import com.eap.eap_order.controller.dto.res.ListUserOrderRes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

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
                "eap.matchEngine.base-url=http://localhost:8082/match-engine"
        })
@EnabledIfSystemProperty(named = "eap.integration.postgres", matches = "true")
class OrderQueryServicePostgresIT {

    @Autowired
    private OrderQueryService service;
    @Autowired
    private JdbcTemplate jdbc;

    private UUID orderId;

    @AfterEach
    void cleanUp() {
        if (orderId != null) {
            jdbc.update("DELETE FROM order_service.order_matching_state WHERE order_id = ?", orderId);
            jdbc.update("DELETE FROM order_service.orders_current WHERE order_id = ?", orderId);
        }
    }

    @Test
    void query_shouldPreferStrongerTradeStateAndExposeReservationState() {
        orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO order_service.orders_current
                    (order_id, user_id, market_id, market_sequence, side, price,
                     original_amount, remaining_amount, matched_amount, status,
                     asset_reservation_status, aggregate_version, created_at, updated_at)
                VALUES (?, ?, 'ENERGY-SPOT', 1, 'BUY', 100,
                        10, 10, 0, 'PENDING_ASSET_CHECK', 'PENDING', 1,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, orderId, userId);
        jdbc.update("""
                INSERT INTO order_service.order_matching_state
                    (order_id, user_id, remaining_amount, matched_amount, status,
                     asset_reservation_status, updated_at)
                VALUES (?, ?, 0, 10, 'MATCHED', 'SUCCEEDED', CURRENT_TIMESTAMP)
                """, orderId, userId);

        ListUserOrderRes all = service.getUserOrderList(userId.toString());

        assertThat(all.getUserOrders()).singleElement().satisfies(order -> {
            assertThat(order.getStatus()).isEqualTo("MATCHED");
            assertThat(order.getAssetReservationStatus()).isEqualTo("SUCCEEDED");
            assertThat(order.getAmount()).isEqualTo(10);
        });
        assertThat(service.getUserPendingOrders(userId.toString()).getUserOrders()).isEmpty();
        assertThat(service.getUserMatchedOrders(userId.toString()).getUserOrders()).hasSize(1);
    }
}
