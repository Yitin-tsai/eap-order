package com.eap.eap_order.application;

import com.eap.common.event.OrderCancellationResultEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import static com.eap.common.constants.RabbitMQConstants.ORDER_ORDER_CANCELLATION_RESULT_QUEUE;

@Component
@RequiredArgsConstructor
public class OrderCancellationResultListener {

    private final OrderCancellationResultInbox inbox;

    @RabbitListener(
            queues = ORDER_ORDER_CANCELLATION_RESULT_QUEUE,
            concurrency = "${eap.order.listeners.order-cancellation-result.concurrency:4}")
    public void onResult(OrderCancellationResultEvent event) {
        if (event == null || event.getCancellationId() == null
                || event.getOrderId() == null || event.getUserId() == null
                || event.getOutcome() == null) {
            throw new IllegalArgumentException("Cancellation result identifiers and outcome are required");
        }
        inbox.receive(event);
    }
}
