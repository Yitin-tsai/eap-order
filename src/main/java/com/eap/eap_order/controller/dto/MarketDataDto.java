package com.eap.eap_order.controller.dto;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 市場數據 DTO
 * 用於通過 WebSocket 傳送實時市場信息
 */
@Data
@Builder
public class MarketDataDto {
    
    /**
     * 最新成交價格
     */
    private Integer lastPrice;
    
    /**
     * 24小時最高價
     */
    private Integer highPrice;
    
    /**
     * 24小時最低價
     */
    private Integer lowPrice;
    
    /**
     * 24小時成交量
     */
    private Long volume;
    
    /**
     * 24小時價格變化
     */
    private Double priceChange;
    
    /**
     * 24小時價格變化百分比
     */
    private Double priceChangePercent;
    
    /**
     * 數據更新時間
     */
    private LocalDateTime timestamp;
    
    /**
     * 市場狀態 (OPEN, CLOSED, SUSPENDED)
     */
    private String marketStatus;
}
