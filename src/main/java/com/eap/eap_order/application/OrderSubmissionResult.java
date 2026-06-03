package com.eap.eap_order.application;

import java.util.UUID;

public record OrderSubmissionResult(UUID orderId, String marketId, Long marketSequence) {
}
