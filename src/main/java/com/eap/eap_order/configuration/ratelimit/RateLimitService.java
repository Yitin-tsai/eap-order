package com.eap.eap_order.configuration.ratelimit;

import com.eap.eap_order.application.OrderSubmissionMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.time.Duration;

@Service
@RequiredArgsConstructor
@Slf4j
public class RateLimitService {

    private final StringRedisTemplate redisTemplate;
    private final OrderSubmissionMetrics metrics;
    @Value("${eap.rate-limit.enabled:true}")
    private boolean enabled;

    private static final String LUA_SCRIPT = """
            local key = KEYS[1]
            local limit = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local now = tonumber(ARGV[3])
            local windowStart = now - window * 1000

            redis.call('ZREMRANGEBYSCORE', key, 0, windowStart)
            local count = redis.call('ZCARD', key)

            if count < limit then
                redis.call('ZADD', key, now, now .. '-' .. math.random(1000000))
                redis.call('PEXPIRE', key, window * 1000)
                return 0
            else
                return 1
            end
            """;

    private final DefaultRedisScript<Long> rateLimitScript = new DefaultRedisScript<>(LUA_SCRIPT, Long.class);

    /**
     * Check if the request should be rate limited.
     *
     * Uses a sliding window algorithm backed by Redis ZSET:
     * - Each request is stored as a member with the current timestamp as score
     * - Expired entries (outside the window) are removed before counting
     * - All operations run in a single Lua script for atomicity
     *
     * @return true if rate limit is exceeded
     */
    public boolean isRateLimited(String userId, int limit, int windowSeconds) {
        long startedNanos = System.nanoTime();
        try {
            return isRateLimitedInternal(userId, limit, windowSeconds);
        } finally {
            metrics.recordRateLimitCheck(Duration.ofNanos(System.nanoTime() - startedNanos));
        }
    }

    private boolean isRateLimitedInternal(String userId, int limit, int windowSeconds) {
        if (!enabled) {
            return false;
        }
        String key = "rate_limit:" + userId;
        long now = System.currentTimeMillis();

        Long result = redisTemplate.execute(
                rateLimitScript,
                Collections.singletonList(key),
                String.valueOf(limit),
                String.valueOf(windowSeconds),
                String.valueOf(now)
        );

        boolean limited = result != null && result == 1;
        if (limited) {
            log.warn("Rate limit exceeded for userId={}, limit={}/{} sec", userId, limit, windowSeconds);
        }
        return limited;
    }
}
