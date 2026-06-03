package com.eap.eap_order.controller.dto.res;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderStateDto {

    private UUID orderId;
    private UUID userId;
    private String marketId;
    private Long marketSequence;
    private String status;
    private Integer price;
    private Integer amount;
    private String orderType;
    private Integer dealPrice;
    private String failReason;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private List<OrderEventDto> timeline;
}
