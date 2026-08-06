package com.eap.eap_order.loadtest;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisRespClientTest {

    @Test
    void commandBytes_usesUtf8ByteLengthForBulkArguments() {
        assertArrayEquals(
                "*2\r\n$3\r\nGET\r\n$6\r\n能源\r\n".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                RedisRespClient.commandBytes("GET", "能源"));
    }

    @Test
    void parseResponse_readsNestedScanResponse() throws Exception {
        assertEquals(
                List.of("0", List.of("key-1", "key-2")),
                RedisRespClient.parseResponse(
                        "*2\r\n$1\r\n0\r\n*2\r\n$5\r\nkey-1\r\n$5\r\nkey-2\r\n"));
    }
}
