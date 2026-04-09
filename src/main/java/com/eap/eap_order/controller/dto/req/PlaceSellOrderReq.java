package com.eap.eap_order.controller.dto.req;

import java.util.UUID;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class PlaceSellOrderReq {

    @NotNull
    private Integer sellPrice;

    @NotNull
    private Integer amount;

    @NotNull
    private UUID seller;


}
