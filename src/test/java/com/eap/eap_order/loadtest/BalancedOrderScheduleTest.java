package com.eap.eap_order.loadtest;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BalancedOrderScheduleTest {

    @Test
    void shuffledScheduleIsBalancedAndReproducible() {
        List<BalancedOrderSchedule.ScheduledOrder> first = BalancedOrderSchedule.create(
                0, 100, BalancedOrderSchedule.ArrivalPattern.SHUFFLED, 20260804L);
        List<BalancedOrderSchedule.ScheduledOrder> second = BalancedOrderSchedule.create(
                0, 100, BalancedOrderSchedule.ArrivalPattern.SHUFFLED, 20260804L);

        assertThat(first).isEqualTo(second).hasSize(200);
        assertThat(first).extracting(BalancedOrderSchedule.ScheduledOrder::side)
                .filteredOn("BUY"::equals)
                .hasSize(100);
        assertThat(first).extracting(BalancedOrderSchedule.ScheduledOrder::side)
                .filteredOn("SELL"::equals)
                .hasSize(100);

        Map<Integer, List<String>> sidesByTrade = first.stream().collect(Collectors.groupingBy(
                BalancedOrderSchedule.ScheduledOrder::tradeIndex,
                Collectors.mapping(BalancedOrderSchedule.ScheduledOrder::side, Collectors.toList())));
        assertThat(sidesByTrade).hasSize(100);
        assertThat(sidesByTrade.values()).allSatisfy(sides ->
                assertThat(sides).containsExactlyInAnyOrder("BUY", "SELL"));
    }

    @Test
    void shuffledScheduleChangesWithSeedAndIsNotStrictlyAlternating() {
        List<BalancedOrderSchedule.ScheduledOrder> first = BalancedOrderSchedule.create(
                0, 100, BalancedOrderSchedule.ArrivalPattern.SHUFFLED, 1L);
        List<BalancedOrderSchedule.ScheduledOrder> second = BalancedOrderSchedule.create(
                0, 100, BalancedOrderSchedule.ArrivalPattern.SHUFFLED, 2L);

        assertThat(first).isNotEqualTo(second);
        assertThat(first).isNotEqualTo(BalancedOrderSchedule.create(
                0, 100, BalancedOrderSchedule.ArrivalPattern.ALTERNATING, 1L));
    }

    @Test
    void shuffledScheduleAssignsUsersRoundRobinByActualSideArrival() {
        List<BalancedOrderSchedule.ScheduledOrder> schedule = BalancedOrderSchedule.create(
                3_500, 5_250, BalancedOrderSchedule.ArrivalPattern.SHUFFLED, 20260807L);

        assertThat(schedule.stream().filter(order -> "BUY".equals(order.side())).toList())
                .extracting(BalancedOrderSchedule.ScheduledOrder::userSequence)
                .containsExactlyElementsOf(sequence(3_500, 5_250));
        assertThat(schedule.stream().filter(order -> "SELL".equals(order.side())).toList())
                .extracting(BalancedOrderSchedule.ScheduledOrder::userSequence)
                .containsExactlyElementsOf(sequence(3_500, 5_250));
    }

    @Test
    void alternatingSchedulePreservesLegacyPairOrder() {
        assertThat(BalancedOrderSchedule.create(
                7, 2, BalancedOrderSchedule.ArrivalPattern.ALTERNATING, 123L))
                .containsExactly(
                        new BalancedOrderSchedule.ScheduledOrder(7, "SELL", 7),
                        new BalancedOrderSchedule.ScheduledOrder(7, "BUY", 7),
                        new BalancedOrderSchedule.ScheduledOrder(8, "SELL", 8),
                        new BalancedOrderSchedule.ScheduledOrder(8, "BUY", 8));
    }

    @Test
    void rejectsUnsupportedArrivalPattern() {
        assertThatThrownBy(() -> BalancedOrderSchedule.ArrivalPattern.parse("sell-first"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shuffled or alternating");
    }

    private static List<Integer> sequence(int start, int size) {
        return IntStream.range(start, start + size).boxed().toList();
    }
}
