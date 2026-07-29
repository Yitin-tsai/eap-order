package com.eap.eap_order.configuration.ratelimit;

import com.eap.eap_order.application.OrderSubmissionMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RateLimitServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private OrderSubmissionMetrics metrics;

    private RateLimitService service;

    @BeforeEach
    void setUp() {
        service = new RateLimitService(redisTemplate, metrics);
        ReflectionTestUtils.setField(service, "enabled", true);
    }

    @Test
    void localFixedWindowCounterAllowsRequestsWithinLimit() {
        assertThat(service.isRateLimited("user-1", 5, 1)).isFalse();
        assertThat(service.isRateLimited("user-1", 5, 1)).isFalse();
        assertThat(service.isRateLimited("user-1", 5, 1)).isFalse();
        assertThat(service.isRateLimited("user-1", 5, 1)).isFalse();

        verify(redisTemplate, never()).execute(any(DefaultRedisScript.class), anyList(), anyString());
    }

    @Test
    void localFixedWindowCounterRejectsRequestsAboveLimit() {
        assertThat(service.isRateLimited("user-1", 2, 1)).isFalse();
        assertThat(service.isRateLimited("user-1", 2, 1)).isFalse();

        assertThat(service.isRateLimited("user-1", 2, 1)).isTrue();
        verify(redisTemplate, never()).execute(any(DefaultRedisScript.class), anyList(), anyString());
        verify(metrics, times(3)).recordRateLimitCheck(any(Duration.class));
    }

    @Test
    void redisFixedWindowCounterCanBeSelected() {
        ReflectionTestUtils.setField(service, "backend", "redis");
        when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
                .thenReturn(6L);

        assertThat(service.isRateLimited("user-1", 5, 1)).isTrue();
        verify(redisTemplate).execute(any(DefaultRedisScript.class), anyList(), anyString());
    }

    @Test
    void disabledRateLimitDoesNotCallRedis() {
        ReflectionTestUtils.setField(service, "enabled", false);

        boolean limited = service.isRateLimited("user-1", 5, 1);

        assertThat(limited).isFalse();
        verify(redisTemplate, never()).execute(any(DefaultRedisScript.class), anyList(), anyString());
        verify(metrics).recordRateLimitCheck(any(Duration.class));
    }

    @Test
    void fixedWindowKeyUsesUserAndTimeBucket() {
        assertThat(RateLimitService.fixedWindowKey("user-1", 1, 1_999L))
                .isEqualTo("rate_limit:user-1:1");
        assertThat(RateLimitService.fixedWindowKey("user-1", 1, 2_000L))
                .isEqualTo("rate_limit:user-1:2");
        assertThat(RateLimitService.fixedWindowKey("user-1", 5, 9_999L))
                .isEqualTo("rate_limit:user-1:1");
    }
}
