package com.eap.eap_order.loadtest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpMatchedTradeCompletionLoadGeneratorTest {

    @Test
    void steadyExpectedOutcome_whenBalanced_shouldExpectNoOpenOrders() {
        var outcome = HttpMatchedTradeCompletionLoadGenerator.SteadyExpectedOutcome.balanced(10);

        assertThat(outcome.acceptedOrders()).isEqualTo(20);
        assertThat(outcome.pairableTrades()).isEqualTo(10);
        assertThat(outcome.unmatchedBuyOrders()).isZero();
        assertThat(outcome.unmatchedSellOrders()).isZero();
    }

    @Test
    void steadyExpectedOutcome_whenDurableSidesDiffer_shouldKeepOnlyTheExcessOpen() {
        var outcome = new HttpMatchedTradeCompletionLoadGenerator.SteadyExpectedOutcome(8, 10);

        assertThat(outcome.acceptedOrders()).isEqualTo(18);
        assertThat(outcome.pairableTrades()).isEqualTo(8);
        assertThat(outcome.unmatchedBuyOrders()).isZero();
        assertThat(outcome.unmatchedSellOrders()).isEqualTo(2);
    }

    @Test
    void steadyExpectedOutcome_whenCountIsNegative_shouldRejectInvalidEvidence() {
        assertThatThrownBy(() ->
                new HttpMatchedTradeCompletionLoadGenerator.SteadyExpectedOutcome(-1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void runDelta_shouldExcludeFactsThatExistedBeforeTraffic() {
        assertThat(HttpMatchedTradeCompletionLoadGenerator.runDelta(135, 100, "trades"))
                .isEqualTo(35);
    }

    @Test
    void runDelta_whenFactsDisappear_shouldRejectTheRun() {
        assertThatThrownBy(() ->
                HttpMatchedTradeCompletionLoadGenerator.runDelta(99, 100, "trades"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("moved below the run baseline");
    }

    @Test
    void backlogGrowth_whenSlopeIsHighButGrowthIsBelowOneSecondOfLoad_shouldIgnoreNoise() {
        boolean exceeded = HttpMatchedTradeCompletionLoadGenerator.exceedsMeaningfulBacklogGrowth(
                43.3, 87, 811, 14.0, 900, 9.0);

        assertThat(exceeded).isFalse();
    }

    @Test
    void backlogGrowth_whenSlopeAndNetGrowthRemainMaterial_shouldFailGate() {
        boolean exceeded = HttpMatchedTradeCompletionLoadGenerator.exceedsMeaningfulBacklogGrowth(
                43.3, 87, 1_200, 14.0, 900, 9.0);

        assertThat(exceeded).isTrue();
    }
}
