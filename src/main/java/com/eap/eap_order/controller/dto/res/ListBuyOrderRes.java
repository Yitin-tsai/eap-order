package com.eap.eap_order.controller.dto.res;

import java.util.List;

import lombok.Data;

@Data
public class ListBuyOrderRes {
    List<buyorder> buyOrders;

    @Data
    public class buyorder {
        private Integer bidPrice;

        private Integer amount;

        private String bidder;

        private String updateTime;

    }
}