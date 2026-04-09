package com.eap.eap_order.controller;

import com.eap.common.event.OrderCancelEvent;
import com.eap.common.dto.*;
import com.eap.eap_order.application.OrderQueryService;
import com.eap.eap_order.application.PlaceBuyOrderService;
import com.eap.eap_order.application.PlaceSellOrderService;
import com.eap.eap_order.application.OutBound.EapMatchEngine;
import com.eap.eap_order.controller.dto.req.PlaceBuyOrderReq;
import com.eap.eap_order.controller.dto.req.PlaceSellOrderReq;
import com.eap.eap_order.controller.dto.res.ListUserOrderRes;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.*;

/**
 * MCP API Controller
 * 專門為 MCP 服務提供的 API 端點
 */
@RestController
@RequestMapping("/mcp/v1")
@Validated
@Tag(name = "MCP API", description = "Model Context Protocol API for LLM Integration")
@Slf4j
public class McpApiController {

    @Autowired
    private PlaceBuyOrderService placeBuyOrderService;
    
    @Autowired
    private PlaceSellOrderService placeSellOrderService;
    
    @Autowired
    private OrderQueryService orderQueryService;
    
    @Autowired
    private EapMatchEngine eapMatchEngine;

    @Operation(summary = "統一下單", description = "支援買賣雙向的統一下單接口")
    @ApiResponse(responseCode = "200", description = "下單成功")
    @PostMapping("/orders")
    public ResponseEntity<PlaceOrderResponse> placeOrder(
            @Valid @RequestBody PlaceOrderRequest request) {
        
        log.info("收到 MCP 下單請求: {}", request);
        
        // 驗證請求
        if (!request.isValid()) {
            String error = request.getValidationError();
            log.warn("下單請求驗證失敗: {}", error);
            return ResponseEntity.badRequest().body(PlaceOrderResponse.failure(error));
        }

        try {
            UUID orderId;
            if (request.isBuy()) {
                PlaceBuyOrderReq buyReq = PlaceBuyOrderReq.builder()
                    .bidPrice(request.getPriceAsInt())
                    .amount(request.getQtyAsInt())
                    .bidder(UUID.fromString(request.getUserId()))
                    .build();
                orderId = placeBuyOrderService.execute(buyReq);
            } else if (request.isSell()) {
                PlaceSellOrderReq sellReq = new PlaceSellOrderReq();
                sellReq.setSellPrice(request.getPriceAsInt());
                sellReq.setAmount(request.getQtyAsInt());
                sellReq.setSeller(UUID.fromString(request.getUserId()));
                orderId = placeSellOrderService.placeSellOrder(sellReq);
            } else {
                return ResponseEntity.badRequest().body(
                    PlaceOrderResponse.failure("Invalid side: " + request.getSide()));
            }

            PlaceOrderResponse response = PlaceOrderResponse.success(
                orderId.toString(), 
                request.getSide(), 
                request.getType(), 
                request.getPrice(), 
                request.getQty(), 
                request.getSymbol()
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("下單失敗", e);
            return ResponseEntity.internalServerError().body(
                PlaceOrderResponse.failure("下單失敗: " + e.getMessage()));
        }
    }

    @Operation(summary = "取消訂單", description = "根據訂單ID取消訂單")
    @ApiResponse(responseCode = "200", description = "取消成功")
    @DeleteMapping("/orders/{orderId}")
    public ResponseEntity<CancelOrderResponse> cancelOrder(
            @Parameter(description = "訂單ID") @PathVariable String orderId) {
        
        log.info("收到 MCP 取消訂單請求: {}", orderId);
        
        try {
            
            OrderCancelEvent cancelEvent = OrderCancelEvent.builder()
                .orderId(UUID.fromString(orderId))
                .build();
            
            
            eapMatchEngine.cancelOrder(cancelEvent);
            
            return ResponseEntity.ok(CancelOrderResponse.success(orderId));
            
        } catch (Exception e) {
            log.error("取消訂單失敗: {}", e.getMessage());
            return ResponseEntity.badRequest().body(
                CancelOrderResponse.failure(orderId, "取消訂單失敗: " + e.getMessage()));
        }
    }

    @Operation(summary = "查詢用戶訂單", description = "根據用戶ID查詢訂單列表")
    @ApiResponse(responseCode = "200", description = "查詢成功")
    @GetMapping("/orders")
    public ResponseEntity<UserOrdersResponse> getUserOrders(
            @Parameter(description = "用戶ID") @RequestParam String userId,
            @Parameter(description = "訂單狀態", required = false) @RequestParam(required = false) String status) {
        
        log.info("收到 MCP 查詢用戶訂單請求: userId={}, status={}", userId, status);
        
        try {
            ListUserOrderRes orders;
            if ("pending".equalsIgnoreCase(status)) {
                orders = orderQueryService.getUserPendingOrders(userId);
            } else if ("matched".equalsIgnoreCase(status)) {
                orders = orderQueryService.getUserMatchedOrders(userId);
            } else {
                orders = orderQueryService.getUserOrderList(userId);
            }
            
            return ResponseEntity.ok(UserOrdersResponse.success(userId, 
                orders.getUserOrders().stream().map(order -> (Object) order).toList(), status));
            
        } catch (Exception e) {
            log.error("查詢用戶訂單失敗: {}", e.getMessage());
            return ResponseEntity.badRequest().body(
                UserOrdersResponse.failure(userId, "查詢失敗: " + e.getMessage()));
        }
    }

 
    @Operation(summary = "獲取訂單簿", description = "獲取當前市場訂單簿數據")
    @ApiResponse(responseCode = "200", description = "獲取成功")
    @GetMapping("/orderbook")
    public ResponseEntity<OrderBookResponseDto> getOrderBook(
            @Parameter(description = "深度") @RequestParam(defaultValue = "10") int depth) {
        
        log.info("收到 MCP 獲取訂單簿請求: depth={}", depth);
        
        try {
            // 限制最大深度以避免回傳過多不需要資訊
            int depthN = Math.min(depth, 20);
            
            // 直接從 MatchEngine 獲取訂單簿數據
            ResponseEntity<OrderBookResponseDto> orderBook = eapMatchEngine.getOrderBook(depthN);
            
            if (orderBook.getStatusCode().is2xxSuccessful() && orderBook.getBody() != null) {
                return ResponseEntity.ok(orderBook.getBody());
            } else {
                // 如果無法獲取數據，返回空的訂單簿
                OrderBookResponseDto emptyOrderBook = OrderBookResponseDto.builder()
                    .bids(new ArrayList<>())
                    .asks(new ArrayList<>())
                    .build();
                return ResponseEntity.ok(emptyOrderBook);
            }
            
        } catch (Exception e) {
            log.error("獲取訂單簿失敗", e);
            OrderBookResponseDto emptyOrderBook = OrderBookResponseDto.builder()
                .bids(new ArrayList<>())
                .asks(new ArrayList<>())
                .build();
            return ResponseEntity.ok(emptyOrderBook);
        }
    }

   
    @Operation(summary = "獲取市場指標", description = "獲取詳細的市場分析指標")
    @ApiResponse(responseCode = "200", description = "獲取成功")
    @GetMapping("/metrics")
    public ResponseEntity<MarketMetricsResponse> getMarketMetrics(
            @Parameter(description = "訂單簿深度") @RequestParam(defaultValue = "10") int depth) {
        
        log.info("收到 MCP 獲取市場指標請求: depth={}", depth);
        
        try {
      
            ResponseEntity<MarketSummaryDto> marketSummary = eapMatchEngine.getMarketSummary();
            
            
            int depthN = Math.min(depth, 20); // 限制最大深度
            ResponseEntity<OrderBookResponseDto> orderBook = eapMatchEngine.getOrderBook(depthN);
            
            Map<String, Object> metrics = new HashMap<>();
            
            if (orderBook.getBody() != null) {
                OrderBookResponseDto orderBookData = orderBook.getBody();
                
                // 計算額外的市場指標
                if (orderBookData.getBids() != null && !orderBookData.getBids().isEmpty() && 
                    orderBookData.getAsks() != null && !orderBookData.getAsks().isEmpty()) {
                    BigDecimal bestBid = BigDecimal.valueOf(orderBookData.getBids().get(0).getPrice());
                    BigDecimal bestAsk = BigDecimal.valueOf(orderBookData.getAsks().get(0).getPrice());
                    BigDecimal spread = bestAsk.subtract(bestBid);
                    BigDecimal spreadPercent = spread.divide(bestAsk, 4, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100));
                    
                    metrics.put("spread", spread);
                    metrics.put("spreadPercent", spreadPercent);
                    metrics.put("bestBid", bestBid);
                    metrics.put("bestAsk", bestAsk);
                    
                    // 流動性指標 (深度N層總量)
                    Integer bidLiquidity = orderBookData.getBids().stream()
                            .map(bid -> bid.getAmount())
                            .reduce(0, Integer::sum);
                    Integer askLiquidity = orderBookData.getAsks().stream()
                            .map(ask -> ask.getAmount())
                            .reduce(0, Integer::sum);
                    
                    metrics.put("bidLiquidity", bidLiquidity);
                    metrics.put("askLiquidity", askLiquidity);
                    metrics.put("totalLiquidity", bidLiquidity + askLiquidity);
                }
            }
            
            return ResponseEntity.ok(MarketMetricsResponse.success(
                marketSummary.getBody(), 
                orderBook.getBody(), 
                metrics, 
                depthN));
            
        } catch (Exception e) {
            log.error("獲取市場指標失敗: {}", e.getMessage());
            return ResponseEntity.badRequest().body(
                MarketMetricsResponse.failure("獲取市場指標失敗: " + e.getMessage()));
        }
    }

    /**
     * 健康檢查
     * GET /mcp/v1/health
     */
    @Operation(summary = "健康檢查", description = "檢查 MCP API 服務狀態")
    @ApiResponse(responseCode = "200", description = "服務正常")
    @GetMapping("/health")
    public ResponseEntity<HealthResponse> health() {
        
        // 檢查依賴服務狀態
        Map<String, String> dependencies = new HashMap<>();
        dependencies.put("placeBuyOrderService", "UP");
        dependencies.put("placeSellOrderService", "UP");
        dependencies.put("orderQueryService", "UP");
        dependencies.put("eapMatchEngine", "UP");
        
        HealthResponse response = HealthResponse.up("mcp-api", "1.0.0");
        response.setDependencies(dependencies);
        
        return ResponseEntity.ok(response);
    }
}
