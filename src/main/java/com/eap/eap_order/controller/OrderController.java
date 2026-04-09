package com.eap.eap_order.controller;

import com.eap.common.event.OrderCancelEvent;
import com.eap.eap_order.application.OrderQueryService;
import com.eap.eap_order.application.PlaceBuyOrderService;
import com.eap.eap_order.application.PlaceSellOrderService;
import com.eap.eap_order.application.OutBound.EapMatchEngine;
import com.eap.eap_order.controller.dto.req.CancelOrderReq;
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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/bid")
@Validated
@Tag(name = "bid", description = "競價系統 API")
@Slf4j
public class OrderController {

    @Autowired
    protected PlaceBuyOrderService placeBuyOrderService;
    @Autowired
    protected PlaceSellOrderService placeSellOrderService;
    @Autowired
    protected OrderQueryService orderQueryService;
    @Autowired
    private  EapMatchEngine eapMatchEngine;

    @Operation(operationId = "post-bid-add", summary = "掛買單", description = "掛單功能 - 買入訂單")
    @ApiResponse(responseCode = "200", description = "掛單成功")
    @ApiResponse(responseCode = "400", description = "請求錯誤")
    @PostMapping("/buy")
    public ResponseEntity<Map<String, Object>> postBidAdd(
            @Parameter(description = "驗證用戶登入") @RequestHeader(value = "ID_TOKEN", required = false) String idToken,
            @Parameter(description = "交易編號") @RequestHeader(value = "txnSEq", required = false) String txnSeq,
            @Parameter(description = "掛買單請求") @Valid @RequestBody PlaceBuyOrderReq request) {

        log.info("掛買單請求: {}", request);
        UUID orderId = placeBuyOrderService.execute(request);

        Map<String, Object> response = new HashMap<>();
        response.put("orderId", orderId);
        response.put("status", "PENDING_WALLET_CHECK");
        response.put("message", "訂單已提交，正在檢查餘額...");

        return ResponseEntity.ok(response);
    }

    @Operation(operationId = "post-bid-sell", summary = "掛賣單", description = "掛單功能 - 賣出訂單")
    @ApiResponse(responseCode = "200", description = "掛單成功")
    @ApiResponse(responseCode = "400", description = "請求錯誤")
    @PostMapping("/sell")
    public ResponseEntity<Void> postBidSell(
            @Parameter(description = "驗證用戶登入") @RequestHeader(value = "ID_TOKEN", required = false) String idToken,
            @Parameter(description = "交易編號") @RequestHeader(value = "txnSEq", required = false) String txnSeq,
            @Parameter(description = "掛賣單請求") @Valid @RequestBody PlaceSellOrderReq request) {

        log.info("掛賣單請求: {}", request);

        placeSellOrderService.placeSellOrder(request);
        return ResponseEntity.ok().build();
    }


    @Operation(operationId = "get-user-orders-all", summary = "查詢用戶全部訂單", description = "取得用戶所有訂單（待處理 + 已成交）")
    @ApiResponse(responseCode = "200", description = "查詢成功")
    @ApiResponse(responseCode = "400", description = "請求錯誤")
    @GetMapping("/user-orders/all")
    public ResponseEntity<ListUserOrderRes> getUserOrdersAll(
            @Parameter(description = "驗證用戶登入") @RequestHeader(value = "ID_TOKEN", required = false) String idToken,
            @Parameter(description = "交易編號") @RequestHeader(value = "txnSeq", required = false) String txnSeq) {

        log.info("查詢用戶全部訂單，idToken: {}, txnSeq: {}", idToken, txnSeq);

        // 使用 OrderQueryService 查詢用戶所有訂單
        ListUserOrderRes response = orderQueryService.getUserOrderList(idToken);
        return ResponseEntity.ok(response);
    }

    @Operation(operationId = "get-user-orders-pending", summary = "查詢平台上的訂單", description = "取得用戶在平台上待處理的訂單")
    @ApiResponse(responseCode = "200", description = "查詢成功")
    @ApiResponse(responseCode = "400", description = "請求錯誤")
    @GetMapping("/user-orders/pending")
    public ResponseEntity<ListUserOrderRes> getUserOrdersPending(
            @Parameter(description = "驗證用戶登入") @RequestHeader(value = "ID_TOKEN", required = false) String idToken,
            @Parameter(description = "交易編號") @RequestHeader(value = "txnSeq", required = false) String txnSeq) {

        log.info("查詢用戶待處理訂單，idToken: {}, txnSeq: {}", idToken, txnSeq);

        ListUserOrderRes response = orderQueryService.getUserPendingOrders(idToken);
        return ResponseEntity.ok(response);
    }

    @Operation(operationId = "get-user-orders-matched", summary = "查詢已成交訂單", description = "取得用戶已成交的訂單")
    @ApiResponse(responseCode = "200", description = "查詢成功")
    @ApiResponse(responseCode = "400", description = "請求錯誤")
    @GetMapping("/user-orders/matched")
    public ResponseEntity<ListUserOrderRes> getUserOrdersMatched(
            @Parameter(description = "驗證用戶登入") @RequestHeader(value = "ID_TOKEN", required = false) String idToken,
            @Parameter(description = "交易編號") @RequestHeader(value = "txnSeq", required = false) String txnSeq) {

        log.info("查詢用戶已成交訂單，idToken: {}, txnSeq: {}", idToken, txnSeq);

        ListUserOrderRes response = orderQueryService.getUserMatchedOrders(idToken);
        return ResponseEntity.ok(response);
    }

    @Operation(operationId = "cancel-order", summary = "取消訂單", description = "用戶取消未完成的訂單")
    @ApiResponse(responseCode = "200", description = "取消成功")
    @ApiResponse(responseCode = "400", description = "請求錯誤")
    @PostMapping("/user-orders/cancel")
    public ResponseEntity<Void> cancelOrder(
            
            @Parameter(description = "取消訂單請求") @Valid @RequestBody CancelOrderReq request) {

        log.info("取消訂單請求: {}", request);

       eapMatchEngine.cancelOrder(OrderCancelEvent.builder().orderId(request.getOrderId()).build());
      return ResponseEntity.ok().build();
    }

}
