package com.eap.eap_order.loadtest;

import java.util.List;
import java.util.UUID;

record ExternalHttpMatchedManifest(
        int manifestSchemaVersion,
        String benchmarkContract,
        String runId,
        String marketId,
        int targetTotalOrderTps,
        int warmupSeconds,
        int measurementSeconds,
        int sampleIntervalSeconds,
        long workloadSeed,
        String arrivalPattern,
        int usersPerSide,
        long expectedHttpOrders,
        long expectedTrades,
        long preparedAtEpochMillis,
        String targetsSha256,
        List<UUID> buyers,
        List<UUID> sellers,
        BalanceSnapshot initialBuyerBalances,
        BalanceSnapshot initialSellerBalances,
        DatabaseBaselineSnapshot databaseBaseline,
        long submissionBaselinePosition) {

    static final int SCHEMA_VERSION = 1;
    static final String CONTRACT = "external-http-matched-steady-state-chain";

    ExternalHttpMatchedManifest {
        buyers = List.copyOf(buyers);
        sellers = List.copyOf(sellers);
    }

    record BalanceSnapshot(
            long availableAmount,
            long lockedAmount,
            long availableCurrency,
            long lockedCurrency) {
    }

    record DatabaseBaselineSnapshot(
            long submitted,
            long reservationClaims,
            long confirmed,
            long matchedOrders,
            long matchTrades,
            long orderTrades,
            long walletTrades) {
    }
}
