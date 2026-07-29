package com.eap.eap_order.configuration.ratelimit;

import com.eap.eap_order.application.OrderSubmissionMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
@Slf4j
public class RateLimitService {

    private final StringRedisTemplate redisTemplate;
    private final OrderSubmissionMetrics metrics;
    @Value("${eap.rate-limit.enabled:true}")
    private boolean enabled;
    @Value("${eap.rate-limit.backend:local}")
    private String backend;

    private final ConcurrentMap<String, LocalWindowCounter> localCounters = new ConcurrentHashMap<>();
    private final AtomicLong localRequestCount = new AtomicLong();

    private static final String LUA_SCRIPT = """
            local key = KEYS[1]
            local ttlMs = tonumber(ARGV[1])
            local current = redis.call('INCR', key)

            if current == 1 then
                redis.call('PEXPIRE', key, ttlMs)
            end

            return current
            """;

    private final DefaultRedisScript<Long> rateLimitScript = new DefaultRedisScript<>(LUA_SCRIPT, Long.class);

    /**
     * Check if the request should be rate limited.
     *
     * Uses a per-instance fixed-window counter by default to keep the order
     * submission hot path off Redis. Set eap.rate-limit.backend=redis when a
     * strict cross-instance user limit is required.
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
        int normalizedWindowSeconds = Math.max(1, windowSeconds);
        int normalizedLimit = Math.max(0, limit);
        long nowMillis = System.currentTimeMillis();

        if ("redis".equalsIgnoreCase(backend)) {
            return isRedisFixedWindowLimited(userId, normalizedLimit, normalizedWindowSeconds, nowMillis);
        }
        return isLocalFixedWindowLimited(userId, normalizedLimit, normalizedWindowSeconds, nowMillis);
    }

    private boolean isRedisFixedWindowLimited(
            String userId,
            int normalizedLimit,
            int normalizedWindowSeconds,
            long nowMillis) {
        String key = fixedWindowKey(userId, normalizedWindowSeconds, nowMillis);
        long ttlMs = normalizedWindowSeconds * 2_000L;

        Long currentCount = redisTemplate.execute(
                rateLimitScript,
                Collections.singletonList(key),
                String.valueOf(ttlMs)
        );

        boolean limited = currentCount != null && currentCount > normalizedLimit;
        if (limited) {
            log.warn("Rate limit exceeded for userId={}, count={}, limit={}/{} sec",
                    userId, currentCount, normalizedLimit, normalizedWindowSeconds);
        }
        return limited;
    }

    private boolean isLocalFixedWindowLimited(
            String userId,
            int normalizedLimit,
            int normalizedWindowSeconds,
            long nowMillis) {
        long bucket = bucket(nowMillis, normalizedWindowSeconds);
        String key = fixedWindowKey(userId, normalizedWindowSeconds, nowMillis);
        LocalWindowCounter counter = localCounters.compute(key, (ignored, existing) -> {
            if (existing == null || existing.bucket() != bucket) {
                return new LocalWindowCounter(bucket, new AtomicInteger(1));
            }
            existing.count().incrementAndGet();
            return existing;
        });

        cleanupLocalCountersOccasionally(bucket);

        int currentCount = counter.count().get();
        boolean limited = currentCount > normalizedLimit;
        if (limited) {
            log.warn("Local rate limit exceeded for userId={}, count={}, limit={}/{} sec",
                    userId, currentCount, normalizedLimit, normalizedWindowSeconds);
        }
        return limited;
    }

    private void cleanupLocalCountersOccasionally(long currentBucket) {
        if ((localRequestCount.incrementAndGet() & 4095L) != 0) {
            return;
        }
        localCounters.entrySet().removeIf(entry -> entry.getValue().bucket() < currentBucket - 1);
    }

    static String fixedWindowKey(String userId, int windowSeconds, long nowMillis) {
        return "rate_limit:" + userId + ":" + bucket(nowMillis, windowSeconds);
    }

    private static long bucket(long nowMillis, int windowSeconds) {
        long windowMillis = Math.max(1, windowSeconds) * 1_000L;
        return Math.floorDiv(nowMillis, windowMillis);
    }

    private record LocalWindowCounter(long bucket, AtomicInteger count) {
    }
}
