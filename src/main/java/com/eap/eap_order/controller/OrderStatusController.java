package com.eap.eap_order.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@RequestMapping("/orders")
@Slf4j
public class OrderStatusController {

    // 只維護SSE連接，不存儲狀態資料
    private final Map<UUID, SseEmitter> orderEmitters = new ConcurrentHashMap<>();
    private final Map<UUID, String> orderStatus = new ConcurrentHashMap<>(); // 內存中的簡單狀態

    /**
     * 建立SSE連接來監聽特定訂單的狀態變化
     */
    @GetMapping(value = "/order/{orderId}/status/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamOrderStatus(@PathVariable UUID orderId) {
        log.info("建立SSE連接監聽訂單狀態: {}", orderId);

        SseEmitter emitter = new SseEmitter(300000L); // 5分鐘超時
        orderEmitters.put(orderId, emitter);

        // 立即發送當前狀態
        try {
            String currentStatus = orderStatus.getOrDefault(orderId, "PENDING_WALLET_CHECK");
            Map<String, Object> statusData = Map.of(
                "orderId", orderId,
                "status", currentStatus,
                "message", getStatusMessage(currentStatus),
                "timestamp", System.currentTimeMillis()
            );

            emitter.send(SseEmitter.event()
                    .name("status-update")
                    .data(statusData));
        } catch (IOException e) {
            log.error("發送初始狀態失敗: {}", e.getMessage());
            emitter.completeWithError(e);
            orderEmitters.remove(orderId);
            return emitter;
        }

        // 處理連接關閉
        emitter.onCompletion(() -> {
            orderEmitters.remove(orderId);
            // 清理完成的訂單狀態
            orderStatus.remove(orderId);
            log.info("SSE連接已關閉: {}", orderId);
        });

        emitter.onTimeout(() -> {
            orderEmitters.remove(orderId);
            log.info("SSE連接超時: {}", orderId);
        });

        emitter.onError((ex) -> {
            orderEmitters.remove(orderId);
            log.error("SSE連接錯誤: {} - {}", orderId, ex.getMessage());
        });

        return emitter;
    }

    /**
     * 接收來自事件監聽器的狀態更新
     */
    public void updateOrderStatus(UUID orderId, String status, String message) {
        orderStatus.put(orderId, status);

        SseEmitter emitter = orderEmitters.get(orderId);
        if (emitter != null) {
            try {
                Map<String, Object> statusData = Map.of(
                    "orderId", orderId,
                    "status", status,
                    "message", message,
                    "timestamp", System.currentTimeMillis()
                );

                emitter.send(SseEmitter.event()
                        .name("status-update")
                        .data(statusData));

                log.info("已推送狀態更新到前端: {} - {}", orderId, status);

                // 如果訂單已完成或失敗，關閉連接
                if ("MATCHED".equals(status) || "INSUFFICIENT_BALANCE".equals(status) || "FAILED".equals(status)) {
                    emitter.complete();
                    orderEmitters.remove(orderId);
                    orderStatus.remove(orderId);
                }
            } catch (IOException e) {
                log.error("推送狀態更新失敗: {}", e.getMessage());
                emitter.completeWithError(e);
                orderEmitters.remove(orderId);
            }
        }
    }

    /**
     * 簡單的狀態查詢API（如果需要的話）
     */
    @GetMapping("/order/{orderId}/status")
    public Map<String, Object> getOrderStatus(@PathVariable UUID orderId) {
    String status = orderStatus.getOrDefault(orderId, "NOT_FOUND");
        return Map.of(
            "orderId", orderId,
            "status", status,
            "message", getStatusMessage(status),
            "timestamp", System.currentTimeMillis()
        );
    }

    private String getStatusMessage(String status) {
        return switch (status) {
            case "PENDING_WALLET_CHECK" -> "正在檢查餘額...";
            case "INSUFFICIENT_BALANCE" -> "餘額不足";
            case "WALLET_CHECK_PASSED" -> "餘額檢查通過，已進入撮合";
            case "MATCHED" -> "撮合成功";
            case "FAILED" -> "處理失敗";
            case "NOT_FOUND" -> "訂單不存在";
            default -> "未知狀態";
        };
    }
}
