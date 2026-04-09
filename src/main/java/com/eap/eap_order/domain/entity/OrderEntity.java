package com.eap.eap_order.domain.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "orders", schema = "order_service")
@Getter
@Setter
@NoArgsConstructor
public class OrderEntity {

    @Id
    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "user_id", nullable = false)
    private UUID userId;

    @Column(name = "price", nullable = false)
    private Integer price;

    @Column(name = "amount", nullable = false)
    private Integer amount;

    @Column(name = "order_type", nullable = false, length = 4)
    private String orderType;

    @Column(name = "status", nullable = false, length = 20)
    private String status = "CONFIRMED";

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt = LocalDateTime.now();
}
