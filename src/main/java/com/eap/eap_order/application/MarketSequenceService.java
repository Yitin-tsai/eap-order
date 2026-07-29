package com.eap.eap_order.application;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
@RequiredArgsConstructor
public class MarketSequenceService {

    public static final String DEFAULT_MARKET_ID = "ENERGY-SPOT";

    private final StringRedisTemplate redisTemplate;
    private final ConcurrentMap<String, SequenceBlock> blocks = new ConcurrentHashMap<>();

    @Value("${eap.order.market-sequence.allocation-block-size:1}")
    private int allocationBlockSize;

    public Long nextSequence(String marketId) {
        int blockSize = Math.max(1, allocationBlockSize);
        if (blockSize == 1) {
            Long sequence = redisTemplate.opsForValue().increment("seq:" + marketId);
            if (sequence == null) {
                throw new IllegalStateException("Failed to allocate market sequence for marketId=" + marketId);
            }
            return sequence;
        }
        return blocks.computeIfAbsent(marketId, ignored -> new SequenceBlock())
                .next(redisTemplate, "seq:" + marketId, blockSize, marketId);
    }

    private static class SequenceBlock {
        private long next;
        private long end;

        synchronized long next(
                StringRedisTemplate redisTemplate,
                String key,
                int blockSize,
                String marketId) {
            if (next > 0 && next <= end) {
                return next++;
            }
            Long newEnd = redisTemplate.opsForValue().increment(key, blockSize);
            if (newEnd == null) {
                throw new IllegalStateException("Failed to allocate market sequence block for marketId=" + marketId);
            }
            long start = newEnd - blockSize + 1;
            next = start + 1;
            end = newEnd;
            return start;
        }
    }
}
