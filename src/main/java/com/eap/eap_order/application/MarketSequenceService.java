package com.eap.eap_order.application;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MarketSequenceService {

    public static final String DEFAULT_MARKET_ID = "ENERGY-SPOT";

    private final StringRedisTemplate redisTemplate;

    public Long nextSequence(String marketId) {
        Long sequence = redisTemplate.opsForValue().increment("seq:" + marketId);
        if (sequence == null) {
            throw new IllegalStateException("Failed to allocate market sequence for marketId=" + marketId);
        }
        return sequence;
    }
}
