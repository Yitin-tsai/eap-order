package com.eap.eap_order.controller.dto.req;

import java.util.UUID;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class CancelOrderReq {

    @NotNull
    private UUID orderId;

    @NotNull
    private UUID userId;

}
