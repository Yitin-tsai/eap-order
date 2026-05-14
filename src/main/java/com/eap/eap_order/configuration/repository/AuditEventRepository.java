package com.eap.eap_order.configuration.repository;

import com.eap.eap_order.domain.entity.AuditEventEntity;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface AuditEventRepository extends JpaRepository<AuditEventEntity, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = "jakarta.persistence.lock.timeout", value = "3000")})
    @Query("SELECT a FROM AuditEventEntity a ORDER BY a.id DESC LIMIT 1")
    Optional<AuditEventEntity> findLatestForUpdate();

    List<AuditEventEntity> findByCorrelationIdOrderByIdAsc(String correlationId);

    List<AuditEventEntity> findByIdBetweenOrderByIdAsc(Long fromId, Long toId);

    List<AuditEventEntity> findByUserIdOrderByIdAsc(UUID userId);
}
