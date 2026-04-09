package com.eap.eap_order.controller.dto.res;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuctionResultRes {

    private String auctionId;
    private Integer clearingPrice;
    private Integer clearingVolume;
    private Integer participantCount;
    private List<UserResult> results;
    private LocalDateTime clearedAt;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserResult {
        private String userId;
        private String side;
        private Integer bidAmount;
        private Integer clearedAmount;
        private Integer settlementAmount;
        private String status;
    }
}
