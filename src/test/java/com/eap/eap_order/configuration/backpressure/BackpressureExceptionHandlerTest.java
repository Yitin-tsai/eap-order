package com.eap.eap_order.configuration.backpressure;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BackpressureExceptionHandlerTest {

    private final BackpressureExceptionHandler handler = new BackpressureExceptionHandler();

    @Test
    void unavailableWalletQueue_shouldReturn503AndRetryAfter() {
        BackpressureRejectedException exception = new BackpressureRejectedException(
                BackpressureRejectedException.Level.UNAVAILABLE, -1, 5, "unavailable");

        ResponseEntity<Map<String, Object>> response = handler.handle(exception);

        assertEquals(503, response.getStatusCode().value());
        assertEquals("5", response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER));
        assertEquals("UNAVAILABLE", response.getBody().get("level"));
    }
}
