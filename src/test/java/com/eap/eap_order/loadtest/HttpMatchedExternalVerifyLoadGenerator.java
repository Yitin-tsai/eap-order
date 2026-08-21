package com.eap.eap_order.loadtest;

public final class HttpMatchedExternalVerifyLoadGenerator {

    private HttpMatchedExternalVerifyLoadGenerator() {
    }

    public static void main(String[] args) throws Exception {
        HttpMatchedTradeCompletionLoadGenerator.verifyExternalSteadyState(args);
    }
}
