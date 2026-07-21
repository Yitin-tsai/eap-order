package com.eap.eap_order.application;

import com.eap.common.event.OrderConfirmedEvent;
import com.eap.common.event.OrderFailedEvent;
import com.eap.eap_order.controller.OrderStatusController;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


import static com.eap.common.constants.RabbitMQConstants.*;

@Component
@Slf4j
public class OrderStatusUpdateListener {

    @Autowired
    private OrderStatusController orderStatusController;

    @Autowired
    private OrderEventSourcingService orderEventSourcingService;

    /**
     * 監聽 Wallet 資產保留成功的 integration event，轉成 OrderAssetReservationConfirmedV1 domain event。
     */
    @RabbitListener(
            queues = ORDER_ORDER_CONFIRMED_QUEUE,
            concurrency = "${eap.order.listeners.asset-reservation-confirmed.concurrency:8}")
    public void onOrderConfirmed(OrderConfirmedEvent event) {
        log.info("收到 Wallet 資產保留成功事件，更新訂單狀態: {}", event.getOrderId());
        orderStatusController.updateOrderStatus(
            event.getOrderId(),
            "WALLET_CHECK_PASSED",
            "餘額檢查通過，已進入撮合佇列"
        );

        orderEventSourcingService.confirm(event);
    }

    /**
     * 監聽 Wallet 資產保留失敗的 integration event，轉成 OrderAssetReservationFailedV1 domain event。
     */
    @RabbitListener(
            queues = ORDER_ORDER_FAILED_QUEUE,
            concurrency = "${eap.order.listeners.asset-reservation-failed.concurrency:4}")
    public void onOrderFailed(OrderFailedEvent failedEvent) {
        log.info("收到訂單失敗通知: {} - {} ({})",
                failedEvent.getOrderId(), failedEvent.getReason(), failedEvent.getFailureType());

        String status = "INSUFFICIENT_BALANCE".equals(failedEvent.getFailureType()) ?
                       "INSUFFICIENT_BALANCE" : "FAILED";

        orderStatusController.updateOrderStatus(
            failedEvent.getOrderId(),
            status,
            failedEvent.getReason()
        );

        orderEventSourcingService.fail(failedEvent);
    }
}
