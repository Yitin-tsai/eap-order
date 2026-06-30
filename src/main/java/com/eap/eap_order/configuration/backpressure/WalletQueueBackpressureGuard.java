package com.eap.eap_order.configuration.backpressure;

import com.eap.common.constants.RabbitMQConstants;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.function.LongSupplier;

@Component
public class WalletQueueBackpressureGuard {

    private final AmqpAdmin amqpAdmin;
    private final WalletQueueBackpressureMetrics metrics;
    private final boolean enabled;
    private final int hardThreshold;
    private final int hardRetryAfterSeconds;
    private final long cacheTtlNanos;
    private final LongSupplier nanoTime;

    private volatile QueueSnapshot cachedSnapshot;

    @Autowired
    public WalletQueueBackpressureGuard(
            AmqpAdmin amqpAdmin,
            WalletQueueBackpressureMetrics metrics,
            @Value("${eap.backpressure.wallet-queue.enabled:true}") boolean enabled,
            @Value("${eap.backpressure.wallet-queue.hard-threshold:10000}") int hardThreshold,
            @Value("${eap.backpressure.wallet-queue.cache-ttl-ms:1000}") long cacheTtlMs,
            @Value("${eap.backpressure.wallet-queue.hard-retry-after-seconds:5}") int hardRetryAfterSeconds) {
        this(amqpAdmin, metrics, enabled, hardThreshold, cacheTtlMs,
                hardRetryAfterSeconds, System::nanoTime);
    }

    WalletQueueBackpressureGuard(
            AmqpAdmin amqpAdmin,
            WalletQueueBackpressureMetrics metrics,
            boolean enabled,
            int hardThreshold,
            long cacheTtlMs,
            int hardRetryAfterSeconds,
            LongSupplier nanoTime) {
        if (hardThreshold <= 0) {
            throw new IllegalArgumentException("Backpressure hard threshold must be positive");
        }
        this.amqpAdmin = amqpAdmin;
        this.metrics = metrics;
        this.enabled = enabled;
        this.hardThreshold = hardThreshold;
        this.hardRetryAfterSeconds = hardRetryAfterSeconds;
        this.cacheTtlNanos = Duration.ofMillis(Math.max(cacheTtlMs, 0)).toNanos();
        this.nanoTime = nanoTime;
    }

    public void checkCanAcceptOrder() {
        if (!enabled) {
            return;
        }

        QueueSnapshot snapshot = currentSnapshot();
        if (!snapshot.available() || snapshot.consumers() <= 0) {
            metrics.hardRejected();
            throw new BackpressureRejectedException(
                    BackpressureRejectedException.Level.UNAVAILABLE,
                    snapshot.depth(),
                    hardRetryAfterSeconds,
                    "Wallet order queue is unavailable or has no active consumer");
        }
        if (snapshot.depth() >= hardThreshold) {
            metrics.hardRejected();
            throw new BackpressureRejectedException(
                    BackpressureRejectedException.Level.HARD,
                    snapshot.depth(),
                    hardRetryAfterSeconds,
                    "Wallet order queue reached the hard backlog threshold");
        }
    }

    private QueueSnapshot currentSnapshot() {
        long now = nanoTime.getAsLong();
        QueueSnapshot snapshot = cachedSnapshot;
        if (snapshot != null && now - snapshot.checkedAtNanos() < cacheTtlNanos) {
            return snapshot;
        }

        synchronized (this) {
            snapshot = cachedSnapshot;
            now = nanoTime.getAsLong();
            if (snapshot != null && now - snapshot.checkedAtNanos() < cacheTtlNanos) {
                return snapshot;
            }
            cachedSnapshot = probeQueue(now);
            return cachedSnapshot;
        }
    }

    private QueueSnapshot probeQueue(long checkedAtNanos) {
        try {
            QueueInformation queue = amqpAdmin.getQueueInfo(RabbitMQConstants.WALLET_ORDER_SUBMITTED_QUEUE);
            if (queue == null) {
                metrics.queueUnavailable();
                return new QueueSnapshot(-1, 0, false, checkedAtNanos);
            }
            metrics.queueObserved(queue.getMessageCount(), queue.getConsumerCount());
            return new QueueSnapshot(queue.getMessageCount(), queue.getConsumerCount(), true, checkedAtNanos);
        } catch (RuntimeException e) {
            metrics.queueUnavailable();
            return new QueueSnapshot(-1, 0, false, checkedAtNanos);
        }
    }

    private record QueueSnapshot(int depth, int consumers, boolean available, long checkedAtNanos) {
    }
}
