package com.eap.eap_order.application;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.eap.common.event.OrderConfirmedEvent;
import com.eap.eap_order.application.OutBound.EapMatchEngine;
import com.eap.eap_order.configuration.repository.MathedOrderRepository;
import com.eap.eap_order.controller.dto.res.ListUserOrderRes;
import com.eap.eap_order.domain.entity.MatchOrderEntity;

@Service
public class OrderQueryService {

    @Autowired
    EapMatchEngine eapMatchEngine;
    
    @Autowired
    MathedOrderRepository matchedOrderRepository;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 查詢用戶的所有訂單（包括待處理和已成交）
     * @param userId 用戶ID
     * @return 用戶訂單列表
     */
    public ListUserOrderRes getUserOrderList(String userId) {
        try {
            List<ListUserOrderRes.UserOrder> allOrders = new ArrayList<>();
            
            // 1. 從 Match-engine Redis 查詢待處理訂單
            List<ListUserOrderRes.UserOrder> pendingOrders = getPendingOrders(userId);
            allOrders.addAll(pendingOrders);
            
            // 2. 從本地 SQL 查詢已成交訂單
            List<ListUserOrderRes.UserOrder> matchedOrders = getMatchedOrders(userId);
            allOrders.addAll(matchedOrders);

            return ListUserOrderRes.builder()
                .userOrders(allOrders)
                .build();
                
        } catch (Exception e) {
            // 記錄錯誤並返回空列表
            // TODO: 加入適當的錯誤處理和日誌記錄
            return ListUserOrderRes.builder()
                .userOrders(List.of())
                .build();
        }
    }

    /**
     * 只查詢用戶的待處理訂單（從 Redis）
     * @param userId 用戶ID
     * @return 待處理訂單列表
     */
    public ListUserOrderRes getUserPendingOrders(String userId) {
        try {
            List<ListUserOrderRes.UserOrder> pendingOrders = getPendingOrders(userId);

            return ListUserOrderRes.builder()
                .userOrders(pendingOrders)
                .build();
                
        } catch (Exception e) {
            return ListUserOrderRes.builder()
                .userOrders(List.of())
                .build();
        }
    }

    /**
     * 只查詢用戶的已成交訂單（從 SQL）
     * @param userId 用戶ID
     * @return 已成交訂單列表
     */
    public ListUserOrderRes getUserMatchedOrders(String userId) {
        try {
            List<ListUserOrderRes.UserOrder> matchedOrders = getMatchedOrders(userId);

            return ListUserOrderRes.builder()
                .userOrders(matchedOrders)
                .build();
                
        } catch (Exception e) {
            return ListUserOrderRes.builder()
                .userOrders(List.of())
                .build();
        }
    }

    /**
     * 從 Match-engine Redis 查詢待處理訂單
     */
    private List<ListUserOrderRes.UserOrder> getPendingOrders(String userId) {
        try {
            List<OrderConfirmedEvent> orderList = eapMatchEngine.queryOrder(userId).getBody();
            
            if (orderList == null || orderList.isEmpty()) {
                return List.of();
            }

            return orderList.stream()
                .map(this::convertPendingOrderToUserOrder)
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            // 如果 Match-engine 查詢失敗，記錄錯誤但不影響已成交訂單查詢
            return List.of();
        }
    }

    /**
     * 從本地 SQL 查詢已成交訂單
     */
    private List<ListUserOrderRes.UserOrder> getMatchedOrders(String userId) {
        try {
            UUID userUuid = UUID.fromString(userId);
            
            // 使用自定義查詢方法查詢用戶的所有成交記錄
            List<MatchOrderEntity> matchedOrders = matchedOrderRepository.findByUserUuid(userUuid);

            return matchedOrders.stream()
                .map(order -> convertMatchedOrderToUserOrder(order, userUuid))
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            // 如果 SQL 查詢失敗，記錄錯誤但不影響待處理訂單查詢
            return List.of();
        }
    }

    /**
     * 轉換待處理訂單（從 Redis）
     */
    private ListUserOrderRes.UserOrder convertPendingOrderToUserOrder(OrderConfirmedEvent event) {
        return ListUserOrderRes.UserOrder.builder()
            .orderId(event.getOrderId() != null ? event.getOrderId().toString() : "")
            .price(event.getPrice())
            .amount(event.getAmount()) // 修正：使用正確的 amount 欄位
            .type(event.getOrderType())
            .updateTime(event.getCreatedAt() != null ? 
                event.getCreatedAt().format(FORMATTER) : "")
            .status("PENDING")
            .build();
    }

    /**
     * 轉換已成交訂單（從 SQL）
     */
    private ListUserOrderRes.UserOrder convertMatchedOrderToUserOrder(MatchOrderEntity entity, UUID userId) {
        // 判斷用戶在此筆交易中的角色（買方或賣方）
        String userRole = entity.getBuyerUuid().equals(userId) ? "BUY" : "SELL";
        
        return ListUserOrderRes.UserOrder.builder()
            .orderId(entity.getId().toString()) // 使用成交記錄的 ID
            .price(entity.getPrice())
            .amount(entity.getAmount())
            .type(userRole) // 用戶在此交易中的角色
            .updateTime(entity.getUpdateTime() != null ? 
                entity.getUpdateTime().format(FORMATTER) : "")
            .status("MATCHED")
            .build();
    }
}
