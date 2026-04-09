package com.eap.eap_order.controller.dto.req;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlaceAuctionBidReq {

    @NotNull
    private UUID userId;

    @NotNull
    private String auctionId;

    @NotNull
    private String side;

    @NotNull
    private List<Step> steps;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Step {
        @NotNull
        private Integer price;

        @NotNull
        private Integer amount;
    }
}
