package com.eap.eap_order.application;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 市場數據定時任務
 * 定期推送市場統計數據和訂單簿數據
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class MarketDataScheduler {
    
    private final MarketDataService marketDataService;
    
    /**
     * 每30分鐘推送一次市場統計數據
     */
    @Scheduled(fixedRate = 30000 * 60)
    public void pushMarketDataPeriodically() {
        try {
            marketDataService.pushMarketData();
        } catch (Exception e) {
            log.error("定期推送市場數據失敗: {}", e.getMessage());
        }
    }
    
    /**
     * 每10分鐘推送一次訂單簿數據
     */
    @Scheduled(fixedRate = 10000*60)
    public void pushOrderBookPeriodically() {
        try {
            marketDataService.pushOrderBook();
        } catch (Exception e) {
            log.error("定期推送訂單簿數據失敗: {}", e.getMessage());
        }
    }
    
    /**
     * 每10分鐘推送一次最近成交資訊
     * 定期為所有訂閱客戶端更新成交歷史
     */
    @Scheduled(fixedRate = 10000 * 60)
    public void pushRecentTradesPeriodically() {
        try {
            marketDataService.pushRecentTrades();
        } catch (Exception e) {
            log.error("定期推送最近成交資訊失敗: {}", e.getMessage());
        }
    }
}
