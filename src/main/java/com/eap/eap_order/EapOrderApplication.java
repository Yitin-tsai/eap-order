package com.eap.eap_order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients
public class EapOrderApplication {

	public static void main(String[] args) {
		SpringApplication.run(EapOrderApplication.class, args);
	}

}
