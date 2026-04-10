package com.eap.eap_order.configuration.repository;

import com.eap.eap_order.domain.entity.AuctionSessionEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface AuctionSessionRepository extends JpaRepository<AuctionSessionEntity, Long> {

    Optional<AuctionSessionEntity> findByAuctionId(String auctionId);

    List<AuctionSessionEntity> findByStatusOrderByCreatedAtDesc(String status);

    List<AuctionSessionEntity> findByStatusOrderByCreatedAtDesc(String status, Pageable pageable);

    Optional<AuctionSessionEntity> findFirstByStatusOrderByCreatedAtDesc(String status);
}
