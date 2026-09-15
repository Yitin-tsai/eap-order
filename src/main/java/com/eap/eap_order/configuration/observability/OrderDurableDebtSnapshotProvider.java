package com.eap.eap_order.configuration.observability;

import com.eap.common.observability.DurableDebtSnapshot;
import com.eap.common.observability.DurableDebtSnapshotCache;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.eap.eap_order.configuration.SchedulingConfig.DURABLE_DEBT_SCHEDULER;

@Component
@Slf4j
public class OrderDurableDebtSnapshotProvider {

    public static final List<String> WORK = List.of(
            "asset_reservation_result_inbox",
            "trade_execution_inbox",
            "cancellation_result_inbox",
            "asset_reservation_released_inbox",
            "event_outbox",
            "orders_current_projection");

    private static final String SQL = """
            SELECT work, total_count, retry_count, terminal_count,
                   oldest_unresolved_age_seconds
            FROM (
                SELECT 'asset_reservation_result_inbox' AS work,
                       count(*) AS total_count,
                       count(*) FILTER (WHERE status = 'FAILED_RETRYABLE'
                           OR (status = 'IN_PROGRESS' AND error_type IS NOT NULL)) AS retry_count,
                       count(*) FILTER (WHERE status = 'FAILED_PERMANENT'
                                            OR conflict_detected_at IS NOT NULL) AS terminal_count,
                       COALESCE(MAX(GREATEST(0, FLOOR(EXTRACT(EPOCH FROM
                           (CURRENT_TIMESTAMP - CASE
                               WHEN conflict_detected_at IS NOT NULL
                                   THEN LEAST(received_at, conflict_detected_at)
                               ELSE received_at END)))::bigint)), 0) AS oldest_unresolved_age_seconds
                FROM order_service.order_asset_reservation_result_inbox
                WHERE status <> 'APPLIED' OR conflict_detected_at IS NOT NULL
                UNION ALL
                SELECT 'trade_execution_inbox', count(*),
                       count(*) FILTER (WHERE status IN ('PENDING_PREREQUISITE', 'FAILED_RETRYABLE')
                           OR (status = 'IN_PROGRESS' AND last_error IS NOT NULL)),
                       count(*) FILTER (WHERE status = 'FAILED_PERMANENT'),
                       COALESCE(MAX(GREATEST(0, FLOOR(EXTRACT(EPOCH FROM
                           (CURRENT_TIMESTAMP - received_at)))::bigint)), 0)
                FROM order_service.order_trade_execution_inbox
                WHERE status <> 'APPLIED'
                UNION ALL
                SELECT 'cancellation_result_inbox', count(*),
                       count(*) FILTER (WHERE status IN ('PENDING_PREREQUISITE', 'FAILED_RETRYABLE')
                           OR (status = 'IN_PROGRESS' AND last_error IS NOT NULL)),
                       count(*) FILTER (WHERE status = 'FAILED_PERMANENT'),
                       COALESCE(MAX(GREATEST(0, FLOOR(EXTRACT(EPOCH FROM
                           (CURRENT_TIMESTAMP - received_at)))::bigint)), 0)
                FROM order_service.order_cancellation_result_inbox
                WHERE status <> 'APPLIED'
                UNION ALL
                SELECT 'asset_reservation_released_inbox', count(*),
                       count(*) FILTER (WHERE status IN ('PENDING_PREREQUISITE', 'FAILED_RETRYABLE')
                           OR (status = 'IN_PROGRESS' AND error_type IS NOT NULL)),
                       count(*) FILTER (WHERE status = 'FAILED_PERMANENT'
                                            OR conflict_detected_at IS NOT NULL),
                       COALESCE(MAX(GREATEST(0, FLOOR(EXTRACT(EPOCH FROM
                           (CURRENT_TIMESTAMP - CASE
                               WHEN conflict_detected_at IS NOT NULL
                                   THEN LEAST(received_at, conflict_detected_at)
                               ELSE received_at END)))::bigint)), 0)
                FROM order_service.order_asset_reservation_released_inbox
                WHERE status <> 'APPLIED' OR conflict_detected_at IS NOT NULL
                UNION ALL
                SELECT 'event_outbox', count(*),
                       count(*) FILTER (WHERE status IN ('PENDING', 'IN_FLIGHT') AND attempt_count > 0),
                       count(*) FILTER (WHERE status = 'FAILED'),
                       COALESCE(MAX(GREATEST(0, FLOOR(EXTRACT(EPOCH FROM
                           (CURRENT_TIMESTAMP - created_at)))::bigint)), 0)
                FROM order_service.order_event_outbox
                WHERE status <> 'SENT'
                UNION ALL
                SELECT 'orders_current_projection',
                       GREATEST(
                           event_debt.event_count,
                           CASE WHEN checkpoint.failure_count > 0 THEN 1 ELSE 0 END),
                       CASE WHEN checkpoint.failure_count > 0 THEN 1 ELSE 0 END,
                       0,
                       GREATEST(
                           COALESCE(GREATEST(0, FLOOR(EXTRACT(EPOCH FROM
                               (CURRENT_TIMESTAMP - event_debt.oldest_occurred_at)))::bigint), 0),
                           COALESCE(GREATEST(0, FLOOR(EXTRACT(EPOCH FROM
                               (CURRENT_TIMESTAMP - checkpoint.first_failure_at)))::bigint), 0))
                FROM (SELECT
                    COALESCE(MAX(last_global_position) FILTER
                        (WHERE projection_name = 'orders_current'), 0) AS last_position,
                    COALESCE(MAX(failure_count) FILTER
                        (WHERE projection_name = 'orders_current'), 0) AS failure_count
                    , MAX(first_failure_at) FILTER
                        (WHERE projection_name = 'orders_current') AS first_failure_at
                    FROM order_service.projection_checkpoints) checkpoint
                CROSS JOIN LATERAL (
                    SELECT count(*) AS event_count,
                           MIN(occurred_at) AS oldest_occurred_at
                    FROM order_service.order_event_store event
                    WHERE event.global_position > checkpoint.last_position
                ) event_debt
            ) debt
            """;

    private final JdbcTemplate jdbc;
    private final DurableDebtSnapshotCache cache =
            new DurableDebtSnapshotCache("eap-order", WORK);
    private final Counter refreshFailures;
    private final AtomicBoolean refreshFailureLogged = new AtomicBoolean();

    public OrderDurableDebtSnapshotProvider(JdbcTemplate jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.refreshFailures = Counter.builder("eap_durable_debt_refresh_failures_total")
                .description("Durable-debt snapshot refresh failures")
                .tag("service", "eap-order")
                .register(registry);
        registerGauges(registry);
    }

    @PostConstruct
    void initialize() {
        refresh();
    }

    @Scheduled(
            fixedDelayString = "${eap.durable-debt.refresh-interval-ms:5000}",
            initialDelayString = "${eap.durable-debt.refresh-interval-ms:5000}",
            scheduler = DURABLE_DEBT_SCHEDULER)
    void refresh() {
        try {
            List<DurableDebtSnapshot.ComponentDebt> components = jdbc.query(SQL, (rs, rowNum) ->
                    new DurableDebtSnapshot.ComponentDebt(
                            rs.getString("work"),
                            rs.getLong("total_count"),
                            rs.getLong("retry_count"),
                            rs.getLong("terminal_count"),
                            rs.getLong("oldest_unresolved_age_seconds")));
            cache.recordSuccess(components);
            if (refreshFailureLogged.getAndSet(false)) {
                log.info("Order durable-debt snapshot refresh recovered");
            }
        } catch (RuntimeException failure) {
            cache.recordFailure();
            refreshFailures.increment();
            if (refreshFailureLogged.compareAndSet(false, true)) {
                log.warn("Could not refresh Order durable-debt snapshot; retaining last successful values", failure);
            }
        }
    }

    public DurableDebtSnapshot snapshot() {
        return cache.snapshot();
    }

    private void registerGauges(MeterRegistry registry) {
        for (String work : WORK) {
            registerCountGauge(registry, work, "total");
            registerCountGauge(registry, work, "retry");
            registerCountGauge(registry, work, "terminal");
            Gauge.builder("eap_durable_debt_oldest_age_seconds", cache,
                            source -> source.component(work).oldestUnresolvedAgeSeconds())
                    .description("Age of the oldest unresolved durable work item")
                    .tags("service", "eap-order", "work", work)
                    .register(registry);
        }
        Gauge.builder("eap_durable_debt_observation_success", cache,
                        source -> source.snapshot().observationSuccess() ? 1 : 0)
                .description("Whether the latest durable-debt observation succeeded")
                .tag("service", "eap-order")
                .register(registry);
        Gauge.builder("eap_durable_debt_snapshot_age_seconds", cache,
                        source -> source.snapshot().snapshotAgeSeconds())
                .description("Age of the last successful durable-debt snapshot")
                .tag("service", "eap-order")
                .register(registry);
        Gauge.builder("eap_durable_debt_contract_version", cache,
                        source -> source.snapshot().contractVersion())
                .description("Durable-debt snapshot contract version")
                .tag("service", "eap-order")
                .register(registry);
    }

    private void registerCountGauge(MeterRegistry registry, String work, String debtClass) {
        Gauge.builder("eap_durable_debt_items", cache, source -> {
                    DurableDebtSnapshot.ComponentDebt debt = source.component(work);
                    return switch (debtClass) {
                        case "total" -> debt.totalCount();
                        case "retry" -> debt.retryCount();
                        case "terminal" -> debt.terminalCount();
                        default -> throw new IllegalStateException("unsupported durable-debt class " + debtClass);
                    };
                })
                .description("Current durable work items by semantic debt class")
                .tags("service", "eap-order", "work", work, "class", debtClass)
                .register(registry);
    }
}
