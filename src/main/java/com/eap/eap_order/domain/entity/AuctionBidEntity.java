package com.eap.eap_order.domain.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "auction_bids", schema = "order_service",
       uniqueConstraints = @UniqueConstraint(
           name = "uk_auction_bids_auction_user",
           columnNames = {"auction_id", "user_id"}))
public class AuctionBidEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "auction_id", nullable = false, length = 20)
    private String auctionId;

    @Column(name = "user_id", nullable = false)
    private UUID userId;

    @Column(name = "side", nullable = false, length = 4)
    private String side;

    @Column(name = "steps", nullable = false, columnDefinition = "jsonb")
    private String steps;

    @Column(name = "total_locked", nullable = false)
    private Integer totalLocked;

    @Column(name = "status", nullable = false, length = 20)
    @Builder.Default
    private String status = "SUBMITTED";

    @Column(name = "created_at", nullable = false)
    @Builder.Default
    private LocalDateTime createdAt = LocalDateTime.now();
}
