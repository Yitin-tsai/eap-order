package com.eap.eap_order.configuration.repository;

import com.eap.eap_order.domain.entity.AuctionBidEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface AuctionBidRepository extends JpaRepository<AuctionBidEntity, Long> {

    List<AuctionBidEntity> findByAuctionId(String auctionId);

    List<AuctionBidEntity> findByAuctionIdAndUserId(String auctionId, UUID userId);

    boolean existsByAuctionIdAndUserId(String auctionId, UUID userId);
}
