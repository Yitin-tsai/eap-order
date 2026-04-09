package com.eap.eap_order.controller.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * 訂單簿數據 DTO
 * 用於通過 WebSocket 推送實時買賣盤數據
 */
@Data
@Builder
public class OrderBookDto {
    
    /**
     * 買盤數據 (價格從高到低排序)
     */
    private List<OrderBookLevel> bids;
    
    /**
     * 賣盤數據 (價格從低到高排序)
     */
    private List<OrderBookLevel> asks;
    
    /**
     * 數據版本號，用於增量更新
     */
    private Long version;
    
    /**
     * 訂單簿層級數據
     */
    @Data
    @Builder
    public static class OrderBookLevel {
        /**
         * 價格
         */
        private Integer price;
        
        /**
         * 該價格的總數量
         */
        private Integer amount;
        
        /**
         * 該價格的訂單數量
         */
        private Integer orderCount;
    }
}
