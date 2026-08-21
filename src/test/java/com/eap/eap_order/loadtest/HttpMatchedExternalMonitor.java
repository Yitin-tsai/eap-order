package com.eap.eap_order.loadtest;

public final class HttpMatchedExternalMonitor {

    private HttpMatchedExternalMonitor() {
    }

    public static void main(String[] args) throws Exception {
        HttpMatchedTradeCompletionLoadGenerator.monitorExternalSteadyState(args);
    }
}
