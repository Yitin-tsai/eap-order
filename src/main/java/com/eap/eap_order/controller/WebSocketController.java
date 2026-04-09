package com.eap.eap_order.controller;

import com.eap.eap_order.application.MarketDataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.annotation.SubscribeMapping;
import org.springframework.stereotype.Controller;

/**
 * WebSocket 控制器
 * 處理客戶端的 WebSocket 訂閱和消息
 */
@Controller
@RequiredArgsConstructor
@Slf4j
public class WebSocketController {
    
    private final MarketDataService marketDataService;
    
    /**
     * 客戶端訂閱市場數據時觸發
     * 立即發送當前市場數據
     */
    @SubscribeMapping("/topic/market")
    public void subscribeMarketData() {
        log.info("客戶端訂閱市場數據");
        // 立即推送當前市場數據
        marketDataService.pushMarketData();
    }
    
    /**
     * 客戶端訂閱實時成交數據時觸發
     * 立即發送最近的成交記錄給新訂閱的客戶端
     */
    @SubscribeMapping("/topic/trades")
    public void subscribeRealtimeTrades() {
        log.info("客戶端訂閱實時成交數據");
        // 發送最近的成交記錄給新訂閱的客戶端
        marketDataService.pushRecentTrades();
    }
    
    /**
     * 客戶端可以單獨訂閱最近成交記錄
     */
    @SubscribeMapping("/topic/trades/recent")
    public void subscribeRecentTrades() {
        log.info("客戶端訂閱最近成交記錄");
        // 立即推送最近成交記錄
        marketDataService.pushRecentTrades();
    }
    
    /**
     * 客戶端訂閱訂單簿數據時觸發
     */
    @SubscribeMapping("/topic/orderbook")
    public void subscribeOrderBook() {
        log.info("客戶端訂閱訂單簿數據");
        // 立即推送當前訂單簿數據
        marketDataService.pushOrderBook();
    }
    
    /**
     * 處理客戶端發送的心跳消息
     */
    @MessageMapping("/heartbeat")
    @SendTo("/topic/heartbeat")
    public String handleHeartbeat(String message) {
        log.debug("收到心跳消息: {}", message);
        return "pong";
    }
    
    /**
     * 處理客戶端請求最新市場數據
     */
    @MessageMapping("/market/refresh")
    public void handleMarketRefresh() {
        log.info("客戶端請求刷新市場數據");
        marketDataService.pushMarketData();
        marketDataService.pushOrderBook();
    }
}
