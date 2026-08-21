package com.eap.eap_order.loadtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ExternalHttpMatchedManifestTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void roundTripsLifecycleEvidenceWithoutConnectionSecrets() throws Exception {
        ExternalHttpMatchedManifest manifest = new ExternalHttpMatchedManifest(
                ExternalHttpMatchedManifest.SCHEMA_VERSION,
                ExternalHttpMatchedManifest.CONTRACT,
                "run-1",
                "ENERGY-SPOT",
                200,
                2,
                10,
                1,
                20260819L,
                "shuffled",
                1,
                2,
                1,
                1_787_000_000_000L,
                "abc123",
                List.of(UUID.fromString("00000000-0000-0000-0000-000000000001")),
                List.of(UUID.fromString("00000000-0000-0000-0000-000000000002")),
                new ExternalHttpMatchedManifest.BalanceSnapshot(1, 2, 3, 4),
                new ExternalHttpMatchedManifest.BalanceSnapshot(5, 6, 7, 8),
                new ExternalHttpMatchedManifest.DatabaseBaselineSnapshot(1, 2, 3, 4, 5, 6, 7),
                9);

        String serialized = objectMapper.writeValueAsString(manifest);
        ExternalHttpMatchedManifest restored = objectMapper.readValue(
                serialized,
                ExternalHttpMatchedManifest.class);

        assertThat(restored).isEqualTo(manifest);
        assertThat(serialized)
                .doesNotContainIgnoringCase("password")
                .doesNotContain("jdbcUrl")
                .doesNotContain("rabbitManagementUrl");
    }
}
