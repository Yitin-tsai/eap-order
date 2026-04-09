package com.eap.eap_order.controller.dto;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 即時成交數據 DTO
 * 用於通過 WebSocket 推送最新成交信息
 */
@Data
@Builder
public class RealtimeTradeDto {
    
    /**
     * 成交價格
     */
    private Integer price;
    
    /**
     * 成交數量
     */
    private Integer amount;
    
    /**
     * 成交時間
     */
    private LocalDateTime tradeTime;
    
    /**
     * 成交類型 (BUY/SELL，表示主動成交方向)
     */
    private String tradeType;
    
    /**
     * 成交ID
     */
    private Long tradeId;
}
