package com.eap.eap_order.application;

import com.eap.common.dto.OrderBookResponseDto;
import com.eap.eap_order.application.OutBound.EapMatchEngine;
import com.eap.eap_order.configuration.repository.MathedOrderRepository;
import com.eap.eap_order.controller.dto.MarketDataDto;
import com.eap.eap_order.controller.dto.OrderBookDto;
import com.eap.eap_order.controller.dto.RealtimeTradeDto;
import com.eap.eap_order.controller.dto.RecentTradesDto;
import com.eap.eap_order.domain.entity.MatchOrderEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 市場數據服務
 * 負責處理實時市場數據的計算和推送
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class MarketDataService {
    
    private final SimpMessagingTemplate messagingTemplate;
    private final MathedOrderRepository matchedOrderRepository;
    private final EapMatchEngine eapMatchEngine;
    
    /**
     * 推送實時成交數據
     */
    public void pushRealtimeTrade(MatchOrderEntity matchOrder) {
        RealtimeTradeDto tradeDto = RealtimeTradeDto.builder()
                .tradeId((long) matchOrder.getId())
                .price(matchOrder.getPrice())
                .amount(matchOrder.getAmount())
                .tradeTime(matchOrder.getUpdateTime())
                .tradeType(matchOrder.getOrderType()) // BUY 或 SELL
                .build();
        
        // 推送到所有訂閱實時成交數據的客戶端
        messagingTemplate.convertAndSend("/topic/trades", tradeDto);
        log.info("推送實時成交數據: 價格={}, 數量={}", tradeDto.getPrice(), tradeDto.getAmount());
    }
    
    /**
     * 推送最近的成交記錄給新訂閱的客戶端
     * 當客戶端首次訂閱 /topic/trades 時調用
     */
    public void pushRecentTrades() {
        try {
            // 獲取最近10筆成交記錄
            List<MatchOrderEntity> recentTrades = matchedOrderRepository.findAll()
                    .stream()
                    .sorted((a, b) -> b.getUpdateTime().compareTo(a.getUpdateTime()))
                    .limit(10)
                    .toList();
            
            if (recentTrades.isEmpty()) {
                log.info("沒有最近的成交記錄");
                return;
            }
            
            // 轉換為 DTO
            List<RealtimeTradeDto> tradeDtos = recentTrades.stream()
                    .map(trade -> RealtimeTradeDto.builder()
                            .tradeId((long) trade.getId())
                            .price(trade.getPrice())
                            .amount(trade.getAmount())
                            .tradeTime(trade.getUpdateTime())
                            .tradeType(trade.getOrderType())
                            .build())
                    .toList();
            
            // 建立批量推送 DTO
            RecentTradesDto recentTradesDto = RecentTradesDto.builder()
                    .trades(tradeDtos)
                    .total(tradeDtos.size())
                    .timestamp(System.currentTimeMillis())
                    .build();
            
            // 推送最近的成交記錄列表
            messagingTemplate.convertAndSend("/topic/trades/recent", recentTradesDto);
            log.info("推送最近 {} 筆成交記錄給新訂閱客戶端", tradeDtos.size());
            
        } catch (Exception e) {
            log.error("推送最近成交記錄失敗: {}", e.getMessage());
        }
    }
    
    /**
     * 推送市場數據統計
     */
    public void pushMarketData() {
        LocalDateTime yesterday = LocalDateTime.now().minusDays(1);
        List<MatchOrderEntity> recentTrades = matchedOrderRepository.findAll()
                .stream()
                .filter(trade -> trade.getUpdateTime().isAfter(yesterday))
                .sorted((a, b) -> b.getUpdateTime().compareTo(a.getUpdateTime()))
                .toList();
        
        if (recentTrades.isEmpty()) {
            log.info("沒有最近的成交記錄，跳過市場數據推送");
            return;
        }
        
        // 計算市場統計數據
        Integer lastPrice = recentTrades.get(0).getPrice();
        Integer highPrice = recentTrades.stream().mapToInt(MatchOrderEntity::getPrice).max().orElse(0);
        Integer lowPrice = recentTrades.stream().mapToInt(MatchOrderEntity::getPrice).min().orElse(0);
        Long volume = recentTrades.stream().mapToLong(MatchOrderEntity::getAmount).sum();
        
        // 計算價格變化 (假設第一筆為昨日收盤價)
        Double priceChange = 0.0;
        Double priceChangePercent = 0.0;
        if (recentTrades.size() > 1) {
            Integer firstPrice = recentTrades.get(recentTrades.size() - 1).getPrice();
            priceChange = (double) (lastPrice - firstPrice);
            priceChangePercent = (priceChange / firstPrice) * 100;
        }
        
        MarketDataDto marketData = MarketDataDto.builder()
                .lastPrice(lastPrice)
                .highPrice(highPrice)
                .lowPrice(lowPrice)
                .volume(volume)
                .priceChange(priceChange)
                .priceChangePercent(priceChangePercent)
                .timestamp(LocalDateTime.now())
                .marketStatus("OPEN")
                .build();
        
        // 推送到所有訂閱市場數據的客戶端
        messagingTemplate.convertAndSend("/topic/market", marketData);
        log.info("推送市場數據: 最新價格={}, 24h成交量={}", lastPrice, volume);
    }
    
    /**
     * 推送訂單簿數據
     * 從 MatchEngine 獲取實時訂單簿數據並推送
     */
    public void pushOrderBook() {
        try {
            // 從 MatchEngine 獲取訂單簿數據
            OrderBookResponseDto orderBookResponse = eapMatchEngine.getOrderBook(10).getBody();
            
            if (orderBookResponse != null) {
                // 轉換為 WebSocket 推送格式
                List<OrderBookDto.OrderBookLevel> bids = orderBookResponse.getBids().stream()
                        .map(level -> OrderBookDto.OrderBookLevel.builder()
                                .price(level.getPrice())
                                .amount(level.getAmount())
                                .orderCount(level.getOrderCount())
                                .build())
                        .toList();
                
                List<OrderBookDto.OrderBookLevel> asks = orderBookResponse.getAsks().stream()
                        .map(level -> OrderBookDto.OrderBookLevel.builder()
                                .price(level.getPrice())
                                .amount(level.getAmount())
                                .orderCount(level.getOrderCount())
                                .build())
                        .toList();
                
                OrderBookDto orderBook = OrderBookDto.builder()
                        .bids(bids)
                        .asks(asks)
                        .version(System.currentTimeMillis())
                        .build();
                
                // 推送到所有訂閱訂單簿的客戶端
                messagingTemplate.convertAndSend("/topic/orderbook", orderBook);
                log.info("推送訂單簿數據，買盤層數: {}, 賣盤層數: {}", bids.size(), asks.size());
            } else {
                log.warn("無法獲取訂單簿數據，跳過推送");
            }
            
        } catch (Exception e) {
            log.error("推送訂單簿數據失敗: {}", e.getMessage());
        }
    }
    
    /**
     * 定期推送市場數據 (可以配合定時任務使用)
     */
    public void scheduleMarketDataPush() {
        pushMarketData();
        pushOrderBook();
    }
}
