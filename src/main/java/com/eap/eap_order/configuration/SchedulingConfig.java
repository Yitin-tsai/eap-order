package com.eap.eap_order.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * 定時任務配置
 */
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "eap.scheduling.enabled", havingValue = "true", matchIfMissing = true)
public class SchedulingConfig {

    public static final String DEFAULT_SCHEDULER = "taskScheduler";
    public static final String ORDER_CANCELLATION_SCHEDULER = "orderCancellationTaskScheduler";

    @Bean(name = DEFAULT_SCHEDULER)
    ThreadPoolTaskScheduler taskScheduler() {
        return singleThreadScheduler("order-scheduler-");
    }

    @Bean(name = ORDER_CANCELLATION_SCHEDULER)
    ThreadPoolTaskScheduler orderCancellationTaskScheduler() {
        return singleThreadScheduler("order-cancellation-");
    }

    private ThreadPoolTaskScheduler singleThreadScheduler(String threadNamePrefix) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix(threadNamePrefix);
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(30);
        return scheduler;
    }
}
