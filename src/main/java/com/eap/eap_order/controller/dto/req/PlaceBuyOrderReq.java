package com.eap.eap_order.controller.dto.req;

import java.util.UUID;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PlaceBuyOrderReq {

  @NotNull private Integer bidPrice;
  @NotNull private Integer amount;
  @NotNull private UUID bidder;
}
