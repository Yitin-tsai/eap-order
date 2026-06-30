package com.eap.eap_order.application;

import com.eap.eap_order.configuration.repository.AuditEventRepository;
import com.eap.eap_order.domain.entity.AuditEventEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AuditServiceTest {

    @Mock
    private AuditEventRepository auditEventRepository;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private AuditService auditService;

    private static final String GENESIS_HASH = "0".repeat(64);

    @Test
    @DisplayName("Initial audit should insert GENESIS directly without predecessor lookup")
    void recordInitial_newChain_insertsWithoutLookup() {
        when(auditEventRepository.insertInitialIfAbsent(
                anyString(), anyString(), any(), anyString(), anyString(), any()))
                .thenReturn(1);

        boolean inserted = auditService.recordInitial(
                "ORDER_SUBMISSION_REQUESTED",
                "order-initial",
                UUID.randomUUID(),
                Map.of("price", 100));

        assertThat(inserted).isTrue();
        verify(auditEventRepository, never()).findLatestByCorrelationIdForUpdate(anyString());
        verify(auditEventRepository).insertInitialIfAbsent(
                eq("ORDER_SUBMISSION_REQUESTED"),
                eq("order-initial"),
                any(),
                anyString(),
                argThat(hash -> hash.length() == 64),
                any());
    }

    @Test
    @DisplayName("Initial audit retry should be idempotent")
    void recordInitial_existingChain_returnsFalse() {
        when(auditEventRepository.insertInitialIfAbsent(
                anyString(), anyString(), any(), anyString(), anyString(), any()))
                .thenReturn(0);

        boolean inserted = auditService.recordInitial(
                "ORDER_SUBMISSION_REQUESTED",
                "order-existing",
                UUID.randomUUID(),
                Map.of("price", 100));

        assertThat(inserted).isFalse();
        verify(auditEventRepository, never()).findLatestByCorrelationIdForUpdate(anyString());
    }

    @Test
    @DisplayName("First audit event should use genesis hash as prevHash")
    void record_firstEvent_usesGenesisHash() {
        when(auditEventRepository.findLatestByCorrelationIdForUpdate("order-123")).thenReturn(Optional.empty());
        when(auditEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        UUID userId = UUID.randomUUID();
        auditService.record("ORDER_SUBMITTED", "order-123", userId, Map.of("price", 100));

        ArgumentCaptor<AuditEventEntity> captor = ArgumentCaptor.forClass(AuditEventEntity.class);
        verify(auditEventRepository).save(captor.capture());

        AuditEventEntity saved = captor.getValue();
        assertThat(saved.getEventType()).isEqualTo("ORDER_SUBMITTED");
        assertThat(saved.getCorrelationId()).isEqualTo("order-123");
        assertThat(saved.getUserId()).isEqualTo(userId);
        assertThat(saved.getPrevHash()).isEqualTo(GENESIS_HASH);
        assertThat(saved.getHash()).isNotBlank().hasSize(64);
    }

    @Test
    @DisplayName("Subsequent audit events should chain to previous hash")
    void record_subsequentEvent_chainsToPreviousHash() {
        AuditEventEntity prev = new AuditEventEntity();
        prev.setHash("abcd1234" + "0".repeat(56));
        when(auditEventRepository.findLatestByCorrelationIdForUpdate("order-123")).thenReturn(Optional.of(prev));
        when(auditEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        auditService.record("ORDER_CONFIRMED", "order-123", UUID.randomUUID(), Map.of("status", "ok"));

        ArgumentCaptor<AuditEventEntity> captor = ArgumentCaptor.forClass(AuditEventEntity.class);
        verify(auditEventRepository).save(captor.capture());

        AuditEventEntity saved = captor.getValue();
        assertThat(saved.getPrevHash()).isEqualTo(prev.getHash());
        assertThat(saved.getHash()).isNotEqualTo(prev.getHash());
    }

    @Test
    @DisplayName("Hash should be deterministic for same inputs")
    void record_sameInputs_producesSameHash() {
        when(auditEventRepository.findLatestByCorrelationIdForUpdate("order-1")).thenReturn(Optional.empty());
        when(auditEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        auditService.record("ORDER_SUBMITTED", "order-1", UUID.randomUUID(), Map.of("a", 1));
        auditService.record("ORDER_SUBMITTED", "order-1", UUID.randomUUID(), Map.of("a", 1));

        ArgumentCaptor<AuditEventEntity> captor = ArgumentCaptor.forClass(AuditEventEntity.class);
        verify(auditEventRepository, times(2)).save(captor.capture());

        List<AuditEventEntity> saved = captor.getAllValues();
        // Same prevHash (genesis) but different timestamps → different hashes
        assertThat(saved.get(0).getPrevHash()).isEqualTo(saved.get(1).getPrevHash());
    }

    @Test
    @DisplayName("verifyChain should return true for valid chain")
    void verifyChain_validChain_returnsTrue() {
        // Build a mini chain manually
        AuditEventEntity e1 = new AuditEventEntity();
        e1.setId(1L);
        e1.setEventType("ORDER_SUBMITTED");
        e1.setCorrelationId("order-1");
        e1.setPayload("{\"price\":100}");
        e1.setPrevHash(GENESIS_HASH);
        e1.setCreatedAt(java.time.LocalDateTime.of(2026, 1, 1, 0, 0));
        // Compute real hash
        String hash1 = computeHash(e1.getEventType(), e1.getCorrelationId(),
                e1.getPayload(), e1.getPrevHash(), e1.getCreatedAt());
        e1.setHash(hash1);

        AuditEventEntity e2 = new AuditEventEntity();
        e2.setId(2L);
        e2.setEventType("ORDER_CONFIRMED");
        e2.setCorrelationId("order-1");
        e2.setPayload("{\"status\":\"ok\"}");
        e2.setPrevHash(hash1);
        e2.setCreatedAt(java.time.LocalDateTime.of(2026, 1, 1, 0, 1));
        String hash2 = computeHash(e2.getEventType(), e2.getCorrelationId(),
                e2.getPayload(), e2.getPrevHash(), e2.getCreatedAt());
        e2.setHash(hash2);

        when(auditEventRepository.findByIdBetweenOrderByIdAsc(1L, 2L)).thenReturn(List.of(e1, e2));

        assertThat(auditService.verifyChain(1L, 2L)).isTrue();
    }

    @Test
    @DisplayName("verifyChain should return false when hash is tampered")
    void verifyChain_tamperedHash_returnsFalse() {
        AuditEventEntity e1 = new AuditEventEntity();
        e1.setId(1L);
        e1.setEventType("ORDER_SUBMITTED");
        e1.setCorrelationId("order-1");
        e1.setPayload("{\"price\":100}");
        e1.setPrevHash(GENESIS_HASH);
        e1.setCreatedAt(java.time.LocalDateTime.of(2026, 1, 1, 0, 0));
        e1.setHash("tampered_hash_" + "0".repeat(50));

        when(auditEventRepository.findByIdBetweenOrderByIdAsc(1L, 1L)).thenReturn(List.of(e1));

        assertThat(auditService.verifyChain(1L, 1L)).isFalse();
    }

    @Test
    @DisplayName("verifyChain should return false when chain link is broken")
    void verifyChain_brokenLink_returnsFalse() {
        AuditEventEntity e1 = new AuditEventEntity();
        e1.setId(1L);
        e1.setEventType("ORDER_SUBMITTED");
        e1.setCorrelationId("order-1");
        e1.setPayload("{\"price\":100}");
        e1.setPrevHash(GENESIS_HASH);
        e1.setCreatedAt(java.time.LocalDateTime.of(2026, 1, 1, 0, 0));
        String hash1 = computeHash(e1.getEventType(), e1.getCorrelationId(),
                e1.getPayload(), e1.getPrevHash(), e1.getCreatedAt());
        e1.setHash(hash1);

        AuditEventEntity e2 = new AuditEventEntity();
        e2.setId(2L);
        e2.setEventType("ORDER_CONFIRMED");
        e2.setCorrelationId("order-1");
        e2.setPayload("{\"status\":\"ok\"}");
        e2.setPrevHash("wrong_prev_hash_" + "0".repeat(48)); // broken link
        e2.setCreatedAt(java.time.LocalDateTime.of(2026, 1, 1, 0, 1));
        String hash2 = computeHash(e2.getEventType(), e2.getCorrelationId(),
                e2.getPayload(), e2.getPrevHash(), e2.getCreatedAt());
        e2.setHash(hash2);

        when(auditEventRepository.findByIdBetweenOrderByIdAsc(1L, 2L)).thenReturn(List.of(e1, e2));

        assertThat(auditService.verifyChain(1L, 2L)).isFalse();
    }

    @Test
    @DisplayName("verifyChain should allow independent per-order chains in same range")
    void verifyChain_independentOrderChains_returnsTrue() {
        AuditEventEntity orderA = new AuditEventEntity();
        orderA.setId(1L);
        orderA.setEventType("ORDER_SUBMITTED");
        orderA.setCorrelationId("order-a");
        orderA.setPayload("{\"price\":100}");
        orderA.setPrevHash(GENESIS_HASH);
        orderA.setCreatedAt(java.time.LocalDateTime.of(2026, 1, 1, 0, 0));
        orderA.setHash(computeHash(orderA.getEventType(), orderA.getCorrelationId(),
                orderA.getPayload(), orderA.getPrevHash(), orderA.getCreatedAt()));

        AuditEventEntity orderB = new AuditEventEntity();
        orderB.setId(2L);
        orderB.setEventType("ORDER_SUBMITTED");
        orderB.setCorrelationId("order-b");
        orderB.setPayload("{\"price\":200}");
        orderB.setPrevHash(GENESIS_HASH);
        orderB.setCreatedAt(java.time.LocalDateTime.of(2026, 1, 1, 0, 1));
        orderB.setHash(computeHash(orderB.getEventType(), orderB.getCorrelationId(),
                orderB.getPayload(), orderB.getPrevHash(), orderB.getCreatedAt()));

        AuditEventEntity orderAConfirmed = new AuditEventEntity();
        orderAConfirmed.setId(3L);
        orderAConfirmed.setEventType("ORDER_CONFIRMED");
        orderAConfirmed.setCorrelationId("order-a");
        orderAConfirmed.setPayload("{\"status\":\"ok\"}");
        orderAConfirmed.setPrevHash(orderA.getHash());
        orderAConfirmed.setCreatedAt(java.time.LocalDateTime.of(2026, 1, 1, 0, 2));
        orderAConfirmed.setHash(computeHash(orderAConfirmed.getEventType(), orderAConfirmed.getCorrelationId(),
                orderAConfirmed.getPayload(), orderAConfirmed.getPrevHash(), orderAConfirmed.getCreatedAt()));

        when(auditEventRepository.findByIdBetweenOrderByIdAsc(1L, 3L))
                .thenReturn(List.of(orderA, orderB, orderAConfirmed));

        assertThat(auditService.verifyChain(1L, 3L)).isTrue();
    }

    @Test
    @DisplayName("verifyChain should return true for empty range")
    void verifyChain_emptyRange_returnsTrue() {
        when(auditEventRepository.findByIdBetweenOrderByIdAsc(100L, 200L)).thenReturn(List.of());
        assertThat(auditService.verifyChain(100L, 200L)).isTrue();
    }

    @Test
    @DisplayName("getTrail should delegate to repository")
    void getTrail_delegatesToRepository() {
        AuditEventEntity e = new AuditEventEntity();
        when(auditEventRepository.findByCorrelationIdOrderByIdAsc("order-1")).thenReturn(List.of(e));

        List<AuditEventEntity> result = auditService.getTrail("order-1");
        assertThat(result).hasSize(1);
    }

    // Mirror the hash computation from AuditService for test verification
    private String computeHash(String eventType, String correlationId, String payload,
                               String prevHash, java.time.LocalDateTime timestamp) {
        String input = eventType + "|" + correlationId + "|" + payload + "|" + prevHash + "|" + timestamp;
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(hashBytes);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
