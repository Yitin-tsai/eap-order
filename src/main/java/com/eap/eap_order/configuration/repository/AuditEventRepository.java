package com.eap.eap_order.configuration.repository;

import com.eap.eap_order.domain.entity.AuditEventEntity;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface AuditEventRepository extends JpaRepository<AuditEventEntity, Long> {

    @Modifying
    @Transactional
    @Query(value = """
            INSERT INTO order_service.audit_events
                (event_type, correlation_id, user_id, payload, prev_hash, hash, created_at)
            VALUES
                (:eventType, :correlationId, :userId, CAST(:payload AS jsonb),
                 repeat('0', 64), :hash, :createdAt)
            ON CONFLICT (correlation_id) WHERE prev_hash = repeat('0', 64)
            DO NOTHING
            """, nativeQuery = true)
    int insertInitialIfAbsent(
            @Param("eventType") String eventType,
            @Param("correlationId") String correlationId,
            @Param("userId") UUID userId,
            @Param("payload") String payload,
            @Param("hash") String hash,
            @Param("createdAt") LocalDateTime createdAt);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = "jakarta.persistence.lock.timeout", value = "3000")})
    @Query("SELECT a FROM AuditEventEntity a ORDER BY a.id DESC LIMIT 1")
    Optional<AuditEventEntity> findLatestForUpdate();

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = "jakarta.persistence.lock.timeout", value = "3000")})
    @Query("SELECT a FROM AuditEventEntity a WHERE a.correlationId = :correlationId ORDER BY a.id DESC LIMIT 1")
    Optional<AuditEventEntity> findLatestByCorrelationIdForUpdate(String correlationId);

    List<AuditEventEntity> findByCorrelationIdOrderByIdAsc(String correlationId);

    List<AuditEventEntity> findByIdBetweenOrderByIdAsc(Long fromId, Long toId);

    List<AuditEventEntity> findByUserIdOrderByIdAsc(UUID userId);
}
