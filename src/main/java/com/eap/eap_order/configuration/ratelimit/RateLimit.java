package com.eap.eap_order.configuration.ratelimit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RateLimit {

    /**
     * SpEL expression to extract the rate limit key (e.g. userId) from method parameters.
     * Example: "#request.bidder" or "#request.userId"
     */
    String key();

    /**
     * Maximum number of requests allowed within the time window.
     */
    int limit() default 10;

    /**
     * Time window in seconds.
     */
    int window() default 1;
}
