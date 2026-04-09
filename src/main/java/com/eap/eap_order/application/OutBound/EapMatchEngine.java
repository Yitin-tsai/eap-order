package com.eap.eap_order.application.OutBound;

import com.eap.common.event.OrderCancelEvent;
import com.eap.common.event.OrderConfirmedEvent;
import com.eap.common.dto.OrderBookResponseDto;
import com.eap.common.dto.MarketSummaryDto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@FeignClient(name = "eap-matchEngine", url = "${eap.matchEngine.base-url}")
public interface EapMatchEngine {

    @DeleteMapping("/v1/order/cancel")
    public boolean cancelOrder(OrderCancelEvent event);

    @GetMapping("/v1/order/query")
    public ResponseEntity<List<OrderConfirmedEvent>> queryOrder(@RequestParam("userId") String userId);
    
    /**
     * 獲取訂單簿數據
     * @param depth 深度（可選，默認10層）
     * @return 訂單簿數據
     */
    @GetMapping("/v1/order/orderbook")
    public ResponseEntity<OrderBookResponseDto> getOrderBook(@RequestParam(value = "depth", defaultValue = "10") int depth);
    
    /**
     * 獲取市場簡要統計
     * @return 最佳買賣價等基本信息
     */
    @GetMapping("/v1/order/market/summary")
    public ResponseEntity<MarketSummaryDto> getMarketSummary();
}
