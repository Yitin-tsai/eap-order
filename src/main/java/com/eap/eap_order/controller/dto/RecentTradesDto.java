package com.eap.eap_order.controller.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * 成交歷史列表 DTO
 * 用於批量推送最近的成交記錄
 */
@Data
@Builder
public class RecentTradesDto {
    
    /**
     * 成交記錄列表
     */
    private List<RealtimeTradeDto> trades;
    
    /**
     * 總數量
     */
    private Integer total;
    
    /**
     * 數據時間戳
     */
    private Long timestamp;
    
    /**
     * 數據類型標識
     */
    @Builder.Default
    private String type = "recent_trades";
}
