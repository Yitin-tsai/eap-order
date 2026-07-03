package com.eap.eap_order.configuration.repository;

import com.eap.eap_order.domain.entity.OrderExecutionLinkEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderExecutionLinkRepository extends JpaRepository<OrderExecutionLinkEntity, Long> {
}
