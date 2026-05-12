package com.eap.eap_order.configuration.ratelimit;

public class RateLimitExceededException extends RuntimeException {

    public RateLimitExceededException(String userId) {
        super("Rate limit exceeded for user: " + userId);
    }
}
