package com.eap.eap_order.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 定時任務配置
 */
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "eap.scheduling.enabled", havingValue = "true", matchIfMissing = true)
public class SchedulingConfig {
}
