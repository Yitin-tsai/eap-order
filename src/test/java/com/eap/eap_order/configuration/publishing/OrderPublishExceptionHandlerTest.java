package com.eap.eap_order.configuration.publishing;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OrderPublishExceptionHandlerTest {

    private final OrderPublishExceptionHandler handler = new OrderPublishExceptionHandler();

    @Test
    void publishFailure_shouldReturnRetryableServiceUnavailableContract() {
        OrderPublishException exception = new OrderPublishException(
                "Order was not accepted because RabbitMQ did not confirm publication",
                new RuntimeException("injected failure")
        );

        ResponseEntity<Map<String, Object>> response = handler.handle(exception);

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        assertEquals("5", response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER));
        assertEquals("ORDER_PUBLISH_UNAVAILABLE", response.getBody().get("error"));
        assertEquals(5, response.getBody().get("retryAfterSeconds"));
    }
}
