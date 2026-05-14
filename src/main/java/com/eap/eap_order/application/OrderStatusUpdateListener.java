package com.eap.eap_order.application;

import com.eap.common.event.OrderConfirmedEvent;
import com.eap.common.event.OrderMatchedEvent;
import com.eap.common.event.OrderFailedEvent;
import com.eap.eap_order.configuration.repository.OrderRepository;
import com.eap.eap_order.controller.OrderStatusController;
import com.eap.eap_order.domain.entity.OrderEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;


import static com.eap.common.constants.RabbitMQConstants.*;

@Component
@Slf4j
public class OrderStatusUpdateListener {

    @Autowired
    private OrderStatusController orderStatusController;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private AuditService auditService;

    /**
     * 監聽OrderConfirmedEvent - 表示wallet檢查通過，訂單已進入撮合
     */
    @RabbitListener(queues = ORDER_ORDER_CONFIRMED_QUEUE)
    public void onOrderConfirmed(OrderConfirmedEvent event) {
        log.info("收到OrderConfirmedEvent，更新訂單狀態: {}", event.getOrderId());
        orderStatusController.updateOrderStatus(
            event.getOrderId(),
            "WALLET_CHECK_PASSED",
            "餘額檢查通過，已進入撮合佇列"
        );

        auditService.record("ORDER_CONFIRMED", event.getOrderId().toString(), event.getUserId(), event);

        // 非同步備份到 orders 表（ADR-003）
        try {
            OrderEntity order = new OrderEntity();
            order.setOrderId(event.getOrderId());
            order.setUserId(event.getUserId());
            order.setPrice(event.getPrice());
            order.setAmount(event.getAmount());
            order.setOrderType(event.getOrderType());
            order.setCreatedAt(event.getCreatedAt());
            order.setStatus("CONFIRMED");
            orderRepository.save(order);
            log.info("訂單已備份到 orders 表: {}", event.getOrderId());
        } catch (DataIntegrityViolationException e) {
            log.warn("訂單備份跳過（已存在）: {}", event.getOrderId());
        } catch (Exception e) {
            log.error("訂單備份失敗（不影響主流程）: {}", event.getOrderId(), e);
        }
    }

    /**
     * 監聽OrderFailedEvent - 表示訂單處理失敗（如餘額不足）
     * Note: Removed duplicate ORDER_MATCHED listener - MatchEventListener handles that event
     */
    @RabbitListener(queues = ORDER_ORDER_FAILED_QUEUE)
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

        auditService.record("ORDER_FAILED", failedEvent.getOrderId().toString(), failedEvent.getUserId(), failedEvent);
    }
}
