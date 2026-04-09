package com.eap.eap_order.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;



/**
 * WebSocket 配置類
 * 用於配置 STOMP 協議的 WebSocket 連接，提供實時市場數據推送
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    /**
     * 配置消息代理
     * /topic - 用於發布訂閱模式，適合市場數據廣播
     * /queue - 用於點對點消息，適合個人訂單狀態
     */
    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        // 啟用簡單內存消息代理，處理 /topic 和 /queue 前綴的消息
        config.enableSimpleBroker("/topic", "/queue");
        
        // 設置應用程序目的地前綴，客戶端發送消息到應用程序的前綴
        config.setApplicationDestinationPrefixes("/app");
        
        // 設置用戶目的地前綴，用於發送給特定用戶的消息
        config.setUserDestinationPrefix("/user");
    }

    /**
     * 註冊 STOMP 端點
     * 客戶端將使用此端點連接到 WebSocket 服務器
     */
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // 註冊 "/ws" 端點，允許跨域，並啟用 SockJS 支持
        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("*") // 允許所有域，生產環境應該限制特定域名
                .withSockJS(); // 啟用 SockJS 支持，提供 WebSocket 的降級選項
    }
}
