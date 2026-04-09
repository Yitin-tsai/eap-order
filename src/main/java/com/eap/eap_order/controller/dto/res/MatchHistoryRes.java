package com.eap.eap_order.controller.dto.res;

import java.util.List;

import lombok.Data;


 @Data
public class MatchHistoryRes {
    
    List<MatchHistory> matchHistories;

    @Data
    public class MatchHistory {
        private Integer matchPrice;
        private Integer amount;
        private String buyer;
        private String seller;
        private String matchTime;

       
    }
}
