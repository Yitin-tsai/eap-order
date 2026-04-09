package com.eap.eap_order.application.OutBound;

import com.eap.common.dto.AuctionBidRequest;
import com.eap.common.dto.AuctionBidResponse;
import com.eap.common.dto.AuctionConfigDto;
import com.eap.common.dto.AuctionStatusDto;
import com.eap.common.event.OrderCancelEvent;
import com.eap.common.event.OrderConfirmedEvent;
import com.eap.common.dto.OrderBookResponseDto;
import com.eap.common.dto.MarketSummaryDto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@FeignClient(name = "eap-matchEngine", url = "${eap.matchEngine.base-url}")
public interface EapMatchEngine {

    @DeleteMapping("/v1/order/cancel")
    boolean cancelOrder(OrderCancelEvent event);

    @GetMapping("/v1/order/query")
    ResponseEntity<List<OrderConfirmedEvent>> queryOrder(@RequestParam("userId") String userId);

    /**
     * 獲取訂單簿數據
     * @param depth 深度（可選，默認10層）
     * @return 訂單簿數據
     */
    @GetMapping("/v1/order/orderbook")
    ResponseEntity<OrderBookResponseDto> getOrderBook(@RequestParam(value = "depth", defaultValue = "10") int depth);

    /**
     * 獲取市場簡要統計
     * @return 最佳買賣價等基本信息
     */
    @GetMapping("/v1/order/market/summary")
    ResponseEntity<MarketSummaryDto> getMarketSummary();

    // --- Auction endpoints ---

    /**
     * Submit auction bid to matchEngine for Redis collection
     */
    @PostMapping("/v1/auction/bid")
    ResponseEntity<AuctionBidResponse> submitAuctionBid(@RequestBody AuctionBidRequest request);

    /**
     * Get current auction status from matchEngine
     */
    @GetMapping("/v1/auction/status")
    ResponseEntity<AuctionStatusDto> getAuctionStatus();

    /**
     * Get auction global config from matchEngine
     */
    @GetMapping("/v1/auction/config")
    ResponseEntity<AuctionConfigDto> getAuctionConfig();

    /**
     * Update auction global config in matchEngine
     */
    @PutMapping("/v1/auction/config")
    ResponseEntity<AuctionConfigDto> updateAuctionConfig(@RequestBody AuctionConfigDto config);
}
