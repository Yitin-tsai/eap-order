package com.eap.eap_order.configuration.backpressure;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.QueueInformation;

import java.util.concurrent.atomic.AtomicLong;

import static com.eap.common.constants.RabbitMQConstants.WALLET_ORDER_SUBMITTED_QUEUE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WalletQueueBackpressureGuardTest {

    @Mock
    private AmqpAdmin amqpAdmin;

    @Mock
    private WalletQueueBackpressureMetrics metrics;

    private AtomicLong nanoTime;

    @BeforeEach
    void setUp() {
        nanoTime = new AtomicLong();
    }

    @Test
    void healthyQueue_shouldAcceptOrderAndCacheProbe() {
        when(amqpAdmin.getQueueInfo(WALLET_ORDER_SUBMITTED_QUEUE))
                .thenReturn(new QueueInformation(WALLET_ORDER_SUBMITTED_QUEUE, 100, 4));
        WalletQueueBackpressureGuard guard = guard(true);

        assertDoesNotThrow(guard::checkCanAcceptOrder);
        assertDoesNotThrow(guard::checkCanAcceptOrder);

        verify(amqpAdmin).getQueueInfo(WALLET_ORDER_SUBMITTED_QUEUE);
        verify(metrics).queueObserved(100, 4);
    }

    @Test
    void hardThreshold_shouldRejectWith503Level() {
        when(amqpAdmin.getQueueInfo(WALLET_ORDER_SUBMITTED_QUEUE))
                .thenReturn(new QueueInformation(WALLET_ORDER_SUBMITTED_QUEUE, 10000, 4));
        WalletQueueBackpressureGuard guard = guard(true);

        BackpressureRejectedException exception = assertThrows(
                BackpressureRejectedException.class,
                guard::checkCanAcceptOrder);

        assertEquals(BackpressureRejectedException.Level.HARD, exception.getLevel());
        assertEquals(5, exception.getRetryAfterSeconds());
        verify(metrics).hardRejected();
    }

    @Test
    void queueWithoutConsumer_shouldFailClosed() {
        when(amqpAdmin.getQueueInfo(WALLET_ORDER_SUBMITTED_QUEUE))
                .thenReturn(new QueueInformation(WALLET_ORDER_SUBMITTED_QUEUE, 0, 0));
        WalletQueueBackpressureGuard guard = guard(true);

        BackpressureRejectedException exception = assertThrows(
                BackpressureRejectedException.class,
                guard::checkCanAcceptOrder);

        assertEquals(BackpressureRejectedException.Level.UNAVAILABLE, exception.getLevel());
        verify(metrics).hardRejected();
    }

    @Test
    void missingQueue_shouldFailClosedAndRecordProbeFailure() {
        when(amqpAdmin.getQueueInfo(WALLET_ORDER_SUBMITTED_QUEUE)).thenReturn(null);
        WalletQueueBackpressureGuard guard = guard(true);

        BackpressureRejectedException exception = assertThrows(
                BackpressureRejectedException.class,
                guard::checkCanAcceptOrder);

        assertEquals(BackpressureRejectedException.Level.UNAVAILABLE, exception.getLevel());
        verify(metrics).queueUnavailable();
    }

    @Test
    void disabledGuard_shouldNotProbeRabbitMq() {
        WalletQueueBackpressureGuard guard = guard(false);

        assertDoesNotThrow(guard::checkCanAcceptOrder);

        verify(amqpAdmin, never()).getQueueInfo(WALLET_ORDER_SUBMITTED_QUEUE);
    }

    private WalletQueueBackpressureGuard guard(boolean enabled) {
        return new WalletQueueBackpressureGuard(
                amqpAdmin,
                metrics,
                enabled,
                10000,
                1000,
                5,
                nanoTime::get
        );
    }
}
