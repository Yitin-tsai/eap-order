package com.eap.eap_order.configuration.repository;

import com.eap.eap_order.domain.entity.MatchOrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface MathedOrderRepository extends JpaRepository<MatchOrderEntity, Long> {

    /**
     * 查詢用戶作為買方或賣方的所有成交記錄
     */
    @Query("SELECT m FROM MatchOrderEntity m WHERE m.buyerUuid = :userId OR m.sellerUuid = :userId ORDER BY m.updateTime DESC")
    List<MatchOrderEntity> findByUserUuid(@Param("userId") UUID userId);

    /**
     * 檢查指定的 matchId 是否已存在（用於冪等性檢查）
     * @param matchId 成交記錄的唯一標識
     * @return true 如果該 matchId 已存在，false 否則
     */
    boolean existsByMatchId(Integer matchId);
}
