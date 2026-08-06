package com.eap.eap_order.application;

import com.eap.common.event.TradeExecutedEvent;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                "eap.wallet.base-url=http://localhost:8081/eap-wallet",
                "eap.matchEngine.base-url=http://localhost:8082/match-engine",
                "eap.order.trade-execution-reconciler.initial-delay-ms=3600000",
                "eap.order.trade-execution-reconciler.poll-interval-ms=3600000"
        })
@EnabledIfSystemProperty(named = "eap.integration.postgres", matches = "true")
class OrderTradeExecutedInboxPostgresIT {

    @Autowired
    private OrderTradeExecutedInbox inbox;
    @Autowired
    private JdbcTemplate jdbc;

    private final List<String> tradeIds = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        for (String tradeId : tradeIds) {
            jdbc.update("DELETE FROM order_service.order_trade_execution_inbox WHERE trade_id = ?", tradeId);
        }
    }

    @Test
    void claimRetryable_shouldLeaseRescheduleAndApplyWithoutDuplicateClaim() {
        TradeExecutedEvent event = event();
        inbox.markPending(List.of(event), new TradeProjectionNotReadyException("lag"));

        List<OrderTradeExecutedInbox.InboxEntry> firstClaim = inbox.claimRetryable(10, "worker-1", 30_000);

        assertEquals(1, firstClaim.size());
        assertTrue(inbox.claimRetryable(10, "worker-2", 30_000).isEmpty());

        inbox.reschedule(firstClaim.get(0), "worker-1", "PENDING_PREREQUISITE",
                new TradeProjectionNotReadyException("still lagging"), 0);
        List<OrderTradeExecutedInbox.InboxEntry> secondClaim = inbox.claimRetryable(10, "worker-2", 30_000);

        assertEquals(1, secondClaim.size());
        assertTrue(inbox.markApplied(event, "worker-2"));
        assertEquals("APPLIED", jdbc.queryForObject(
                "SELECT status FROM order_service.order_trade_execution_inbox WHERE trade_id = ?",
                String.class, event.getTradeId()));
    }

    @Test
    void retryPermanentFailure_shouldReturnEventToRetryQueue() {
        TradeExecutedEvent event = event();
        inbox.markPermanentFailure(List.of(event), new TradeApplicationRejectedException("invalid"));

        assertTrue(inbox.retryPermanentFailure(event.getTradeId()));

        List<OrderTradeExecutedInbox.InboxEntry> claimed = inbox.claimRetryable(10, "worker", 30_000);
        assertEquals(1, claimed.size());
        assertEquals(event.getTradeId(), claimed.get(0).event().getTradeId());
    }

    private TradeExecutedEvent event() {
        String tradeId = "inbox-it-" + UUID.randomUUID();
        tradeIds.add(tradeId);
        return TradeExecutedEvent.builder()
                .tradeId(tradeId)
                .legacyMatchId(1)
                .buyerOrderId(UUID.randomUUID())
                .sellerOrderId(UUID.randomUUID())
                .dealPrice(100)
                .quantity(10)
                .occurredAt(LocalDateTime.now())
                .build();
    }
}
