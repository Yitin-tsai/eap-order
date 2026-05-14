package com.eap.eap_order.application;

import com.eap.eap_order.configuration.repository.AuditEventRepository;
import com.eap.eap_order.domain.entity.AuditEventEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class AuditService {

    private final AuditEventRepository auditEventRepository;
    private final ObjectMapper objectMapper;

    private static final String GENESIS_HASH = "0".repeat(64);

    @Transactional
    public void record(String eventType, String correlationId, UUID userId, Object payload) {
        String prevHash = auditEventRepository.findLatestForUpdate()
                .map(AuditEventEntity::getHash)
                .orElse(GENESIS_HASH);

        String payloadJson = serializePayload(payload);
        LocalDateTime now = LocalDateTime.now();

        String hash = computeHash(eventType, correlationId, payloadJson, prevHash, now);

        AuditEventEntity entity = new AuditEventEntity();
        entity.setEventType(eventType);
        entity.setCorrelationId(correlationId);
        entity.setUserId(userId);
        entity.setPayload(payloadJson);
        entity.setPrevHash(prevHash);
        entity.setHash(hash);
        entity.setCreatedAt(now);

        auditEventRepository.save(entity);
        log.debug("Audit recorded: type={}, correlationId={}, hash={}", eventType, correlationId, hash.substring(0, 8));
    }

    public List<AuditEventEntity> getTrail(String correlationId) {
        return auditEventRepository.findByCorrelationIdOrderByIdAsc(correlationId);
    }

    public boolean verifyChain(Long fromId, Long toId) {
        List<AuditEventEntity> events = auditEventRepository.findByIdBetweenOrderByIdAsc(fromId, toId);
        if (events.isEmpty()) {
            return true;
        }

        for (int i = 0; i < events.size(); i++) {
            AuditEventEntity event = events.get(i);

            String expectedHash = computeHash(
                    event.getEventType(),
                    event.getCorrelationId(),
                    event.getPayload(),
                    event.getPrevHash(),
                    event.getCreatedAt()
            );

            if (!expectedHash.equals(event.getHash())) {
                log.error("Hash chain broken at audit event id={}, expected={}, actual={}",
                        event.getId(), expectedHash, event.getHash());
                return false;
            }

            if (i > 0) {
                AuditEventEntity prev = events.get(i - 1);
                if (!event.getPrevHash().equals(prev.getHash())) {
                    log.error("Hash chain link broken between id={} and id={}",
                            prev.getId(), event.getId());
                    return false;
                }
            }
        }
        return true;
    }

    private String computeHash(String eventType, String correlationId, String payload,
                               String prevHash, LocalDateTime timestamp) {
        String input = eventType + "|" + correlationId + "|" + payload + "|" + prevHash + "|" + timestamp;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    private String serializePayload(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize audit payload", e);
        }
    }
}
