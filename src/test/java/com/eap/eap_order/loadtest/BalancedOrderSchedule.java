package com.eap.eap_order.loadtest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

final class BalancedOrderSchedule {

    private static final long PHASE_SEED_MULTIPLIER = 0x9E3779B97F4A7C15L;

    private BalancedOrderSchedule() {
    }

    static List<ScheduledOrder> create(
            int startingTradeIndex,
            int trades,
            ArrivalPattern pattern,
            long workloadSeed) {
        if (startingTradeIndex < 0) {
            throw new IllegalArgumentException("startingTradeIndex must be non-negative");
        }
        if (trades < 0) {
            throw new IllegalArgumentException("trades must be non-negative");
        }

        List<OrderIdentity> orders = new ArrayList<>(Math.multiplyExact(trades, 2));
        for (int offset = 0; offset < trades; offset++) {
            int tradeIndex = Math.addExact(startingTradeIndex, offset);
            orders.add(new OrderIdentity(tradeIndex, "SELL"));
            orders.add(new OrderIdentity(tradeIndex, "BUY"));
        }
        if (pattern == ArrivalPattern.SHUFFLED) {
            long phaseSeed = workloadSeed ^ (PHASE_SEED_MULTIPLIER * startingTradeIndex);
            Collections.shuffle(orders, new Random(phaseSeed));
        }

        int nextBuyUserSequence = startingTradeIndex;
        int nextSellUserSequence = startingTradeIndex;
        List<ScheduledOrder> schedule = new ArrayList<>(orders.size());
        for (OrderIdentity order : orders) {
            int userSequence = "BUY".equals(order.side())
                    ? nextBuyUserSequence++
                    : nextSellUserSequence++;
            schedule.add(new ScheduledOrder(order.tradeIndex(), order.side(), userSequence));
        }
        return List.copyOf(schedule);
    }

    enum ArrivalPattern {
        SHUFFLED("shuffled"),
        ALTERNATING("alternating");

        private final String externalName;

        ArrivalPattern(String externalName) {
            this.externalName = externalName;
        }

        static ArrivalPattern parse(String value) {
            String normalized = value.trim().toLowerCase(Locale.ROOT);
            for (ArrivalPattern pattern : values()) {
                if (pattern.externalName.equals(normalized)) {
                    return pattern;
                }
            }
            throw new IllegalArgumentException(
                    "--arrival-pattern must be shuffled or alternating");
        }

        String externalName() {
            return externalName;
        }
    }

    private record OrderIdentity(int tradeIndex, String side) {
    }

    record ScheduledOrder(int tradeIndex, String side, int userSequence) {
    }
}
