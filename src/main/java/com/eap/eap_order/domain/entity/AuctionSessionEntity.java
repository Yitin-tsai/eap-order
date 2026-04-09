package com.eap.eap_order.domain.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "auction_sessions", schema = "order_service")
public class AuctionSessionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "auction_id", nullable = false, unique = true, length = 20)
    private String auctionId;

    @Column(name = "status", nullable = false, length = 20)
    private String status;

    @Column(name = "delivery_hour", nullable = false)
    private LocalDateTime deliveryHour;

    @Column(name = "open_time", nullable = false)
    private LocalDateTime openTime;

    @Column(name = "close_time", nullable = false)
    private LocalDateTime closeTime;

    @Column(name = "clearing_price")
    private Integer clearingPrice;

    @Column(name = "clearing_volume")
    private Integer clearingVolume;

    @Column(name = "participant_count")
    @Builder.Default
    private Integer participantCount = 0;

    @Column(name = "price_floor", nullable = false)
    private Integer priceFloor;

    @Column(name = "price_ceiling", nullable = false)
    private Integer priceCeiling;

    @Column(name = "created_at", nullable = false)
    @Builder.Default
    private LocalDateTime createdAt = LocalDateTime.now();
}
