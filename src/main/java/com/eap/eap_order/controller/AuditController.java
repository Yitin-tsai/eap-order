package com.eap.eap_order.controller;

import com.eap.eap_order.application.AuditService;
import com.eap.eap_order.application.OrderReplayService;
import com.eap.eap_order.controller.dto.res.OrderStateDto;
import com.eap.eap_order.domain.entity.AuditEventEntity;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/audit")
@RequiredArgsConstructor
@Tag(name = "audit", description = "Audit Trail API")
public class AuditController {

    private final AuditService auditService;
    private final OrderReplayService orderReplayService;

    @Operation(summary = "Query audit trail by correlation ID")
    @GetMapping("/trail/{correlationId}")
    public ResponseEntity<List<AuditEventEntity>> getTrail(@PathVariable String correlationId) {
        List<AuditEventEntity> trail = auditService.getTrail(correlationId);
        return ResponseEntity.ok(trail);
    }

    @Operation(summary = "Verify hash chain integrity for a range of audit events")
    @GetMapping("/verify")
    public ResponseEntity<Map<String, Object>> verifyChain(
            @RequestParam Long fromId,
            @RequestParam Long toId) {
        boolean valid = auditService.verifyChain(fromId, toId);
        return ResponseEntity.ok(Map.of(
                "valid", valid,
                "fromId", fromId,
                "toId", toId
        ));
    }

    @Operation(summary = "Replay order state from audit events")
    @GetMapping("/orders/{orderId}/history")
    public ResponseEntity<OrderStateDto> getOrderHistory(@PathVariable String orderId) {
        OrderStateDto state = orderReplayService.replay(orderId);
        if (state == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(state);
    }

    @Operation(summary = "Replay all order states for a user")
    @GetMapping("/orders/user/{userId}")
    public ResponseEntity<List<OrderStateDto>> getUserOrders(@PathVariable UUID userId) {
        List<OrderStateDto> orders = orderReplayService.replayByUser(userId);
        return ResponseEntity.ok(orders);
    }
}
