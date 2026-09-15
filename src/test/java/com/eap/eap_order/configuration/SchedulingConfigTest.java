package com.eap.eap_order.configuration;

import com.eap.eap_order.configuration.observability.OrderDurableDebtSnapshotProvider;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulingConfigTest {

    @Test
    void slowDurableDebtRefreshCannotBlockDefaultBusinessScheduler() throws Exception {
        SchedulingConfig config = new SchedulingConfig();
        ThreadPoolTaskScheduler defaultScheduler = config.taskScheduler();
        ThreadPoolTaskScheduler durableDebtScheduler = config.durableDebtTaskScheduler();
        defaultScheduler.initialize();
        durableDebtScheduler.initialize();
        CountDownLatch refreshStarted = new CountDownLatch(1);
        CountDownLatch releaseRefresh = new CountDownLatch(1);
        try {
            durableDebtScheduler.submit(() -> {
                refreshStarted.countDown();
                releaseRefresh.await(5, TimeUnit.SECONDS);
                return null;
            });
            assertThat(refreshStarted.await(1, TimeUnit.SECONDS)).isTrue();

            String businessThread = defaultScheduler
                    .submit(() -> Thread.currentThread().getName())
                    .get(1, TimeUnit.SECONDS);

            assertThat(businessThread).startsWith("order-scheduler-");
        } finally {
            releaseRefresh.countDown();
            durableDebtScheduler.shutdown();
            defaultScheduler.shutdown();
        }
    }

    @Test
    void assignsDurableDebtRefreshToDedicatedScheduler() throws Exception {
        Method method = OrderDurableDebtSnapshotProvider.class.getDeclaredMethod("refresh");
        Scheduled scheduled = method.getAnnotation(Scheduled.class);

        assertThat(scheduled).isNotNull();
        assertThat(scheduled.scheduler()).isEqualTo(SchedulingConfig.DURABLE_DEBT_SCHEDULER);
    }
}
