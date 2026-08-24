package com.eap.eap_order.controller;

import com.eap.eap_order.application.AuditService;
import com.eap.eap_order.domain.entity.AuditEventEntity;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/audit")
@RequiredArgsConstructor
@Tag(name = "audit", description = "Audit Trail API")
public class AuditController {

    private final AuditService auditService;

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
}
