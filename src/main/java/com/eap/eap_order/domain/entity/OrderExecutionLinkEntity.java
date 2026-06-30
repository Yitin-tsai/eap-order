package com.eap.eap_order.domain.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "order_execution_links", schema = "order_service")
@Getter
@NoArgsConstructor
public class OrderExecutionLinkEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "trade_id", nullable = false, length = 80)
    private String tradeId;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "side", nullable = false, length = 4)
    private String side;

    @Column(name = "price", nullable = false)
    private Integer price;

    @Column(name = "quantity", nullable = false)
    private Integer quantity;

    @Column(name = "applied_at", nullable = false)
    private LocalDateTime appliedAt = LocalDateTime.now();

    public OrderExecutionLinkEntity(String tradeId, UUID orderId, String side, Integer price, Integer quantity, LocalDateTime appliedAt) {
        this.tradeId = tradeId;
        this.orderId = orderId;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        this.appliedAt = appliedAt == null ? LocalDateTime.now() : appliedAt;
    }
}
