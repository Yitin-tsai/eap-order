package com.eap.eap_order.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 診斷控制器，用於檢查 Spring Bean 狀態
 */
@RestController
@RequestMapping("/debug")
public class DebugController {

    @Autowired
    private ApplicationContext applicationContext;

    @GetMapping("/beans")
    public ResponseEntity<Map<String, Object>> getBeans() {
        Map<String, Object> response = new HashMap<>();
        
        // 檢查所有 Bean 名稱
        String[] beanNames = applicationContext.getBeanDefinitionNames();
        response.put("totalBeans", beanNames.length);
        
        // 檢查特定的服務 Bean
        Map<String, Boolean> serviceStatus = new HashMap<>();
        serviceStatus.put("PlaceBuyOrderService", checkBean("placeBuyOrderService"));
        serviceStatus.put("PlaceSellOrderService", checkBean("placeSellOrderService"));
        serviceStatus.put("OrderQueryService", checkBean("orderQueryService"));
        serviceStatus.put("EapMatchEngine", checkBean("eapMatchEngine"));
        serviceStatus.put("McpApiController", checkBean("mcpApiController"));
        
        response.put("requiredServices", serviceStatus);
        
        // 列出所有控制器 Bean
        response.put("controllers", Arrays.stream(beanNames)
            .filter(name -> name.toLowerCase().contains("controller"))
            .toArray());
            
        return ResponseEntity.ok(response);
    }
    
    private boolean checkBean(String beanName) {
        try {
            return applicationContext.containsBean(beanName);
        } catch (Exception e) {
            return false;
        }
    }
}
