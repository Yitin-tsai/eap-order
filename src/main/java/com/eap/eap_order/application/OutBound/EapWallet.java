package com.eap.eap_order.application.OutBound;


import com.eap.common.event.OrderConfirmedEvent;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = "eap-wallet", url = "${eap.wallet.base-url}")
public interface EapWallet {

    @PostMapping("/v1/wallet/check")
    public boolean checkWallet(OrderConfirmedEvent event) ;
}
