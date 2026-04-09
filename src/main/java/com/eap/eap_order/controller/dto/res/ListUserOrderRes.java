package com.eap.eap_order.controller.dto.res;

import java.util.List;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ListUserOrderRes {
    private List<UserOrder> userOrders;

    @Data
    @Builder
    public static class UserOrder {
        private Integer price;
        private Integer amount;
        private String type;
        private String updateTime;
        private String orderId;
        private String status;
    }
}
