package com.eap.eap_order.controller.dto.req;

import java.util.UUID;

import lombok.Data;

@Data
public class CancelOrderReq {

    private UUID orderId;

}
