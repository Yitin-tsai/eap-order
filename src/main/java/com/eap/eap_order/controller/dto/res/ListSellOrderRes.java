package com.eap.eap_order.controller.dto.res;

import java.util.List;

import lombok.Data;

 @Data
public class ListSellOrderRes {

    List<sellorder> sellOrders;

    @Data
    public class sellorder {
        private Integer bidPrice;

        private Integer amount;

        private String seller;

        private String updateTime;

    }
    
}
