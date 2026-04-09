package com.eap.eap_order.configuration.repository;

import com.eap.eap_order.domain.entity.AuctionResultEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface AuctionResultRepository extends JpaRepository<AuctionResultEntity, Long> {

    List<AuctionResultEntity> findByAuctionId(String auctionId);

    List<AuctionResultEntity> findByAuctionIdAndUserId(String auctionId, UUID userId);

    List<AuctionResultEntity> findByUserId(UUID userId);
}
