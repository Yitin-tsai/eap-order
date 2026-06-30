package com.eap.eap_order.configuration.backpressure;

public class BackpressureRejectedException extends RuntimeException {

    public enum Level {
        HARD,
        UNAVAILABLE
    }

    private final Level level;
    private final int queueDepth;
    private final int retryAfterSeconds;

    public BackpressureRejectedException(
            Level level,
            int queueDepth,
            int retryAfterSeconds,
            String message) {
        super(message);
        this.level = level;
        this.queueDepth = queueDepth;
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public Level getLevel() {
        return level;
    }

    public int getQueueDepth() {
        return queueDepth;
    }

    public int getRetryAfterSeconds() {
        return retryAfterSeconds;
    }
}
