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
@Table(name = "auction_results", schema = "order_service")
public class AuctionResultEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "auction_id", nullable = false, length = 20)
    private String auctionId;

    @Column(name = "user_id", nullable = false)
    private UUID userId;

    @Column(name = "side", nullable = false, length = 4)
    private String side;

    @Column(name = "clearing_price", nullable = false)
    private Integer clearingPrice;

    @Column(name = "bid_amount", nullable = false)
    private Integer bidAmount;

    @Column(name = "cleared_amount", nullable = false)
    private Integer clearedAmount;

    @Column(name = "settlement_amount", nullable = false)
    private Integer settlementAmount;

    @Column(name = "created_at", nullable = false)
    @Builder.Default
    private LocalDateTime createdAt = LocalDateTime.now();
}
