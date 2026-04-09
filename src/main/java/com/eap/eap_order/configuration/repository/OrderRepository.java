package com.eap.eap_order.configuration.repository;

import com.eap.eap_order.domain.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<OrderEntity, UUID> {

    List<OrderEntity> findByStatus(String status);
}
