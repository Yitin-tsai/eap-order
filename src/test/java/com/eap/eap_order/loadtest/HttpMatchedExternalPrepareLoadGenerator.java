package com.eap.eap_order.loadtest;

public final class HttpMatchedExternalPrepareLoadGenerator {

    private HttpMatchedExternalPrepareLoadGenerator() {
    }

    public static void main(String[] args) throws Exception {
        HttpMatchedTradeCompletionLoadGenerator.prepareExternalSteadyState(args);
    }
}
