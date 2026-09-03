package com.eap.eap_order.loadtest;

import org.junit.jupiter.api.Test;

import java.util.List;

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

    @Test
    void backlogSummary_ignoresUnavailableQueueSamples() {
        var summary = HttpMatchedTradeCompletionLoadGenerator.summarizeBacklog(List.of(
                sample(0, 10, 0),
                sample(1, RabbitManagementClient.UNAVAILABLE_BACKLOG, 7),
                sample(2, 30, 0)));

        assertThat(summary.start()).isEqualTo(10);
        assertThat(summary.end()).isEqualTo(30);
        assertThat(summary.max()).isEqualTo(30);
        assertThat(summary.slopePerSecond()).isEqualTo(10.0);
        assertThat(summary.validSamples()).isEqualTo(2);
    }

    @Test
    void backlogSummary_whenAllSamplesUnavailable_reportsNoBacklogEvidence() {
        var summary = HttpMatchedTradeCompletionLoadGenerator.summarizeBacklog(List.of(
                sample(0, RabbitManagementClient.UNAVAILABLE_BACKLOG, 7)));

        assertThat(summary.start()).isEqualTo(-1);
        assertThat(summary.end()).isEqualTo(-1);
        assertThat(summary.max()).isEqualTo(-1);
        assertThat(summary.slopePerSecond()).isZero();
        assertThat(summary.validSamples()).isZero();
    }

    @Test
    void orderReservationInboxBacklogSummary_isIndependentFromRabbitQueueBacklog() {
        var summary = HttpMatchedTradeCompletionLoadGenerator
                .summarizeOrderReservationInboxBacklog(List.of(
                        sample(0, 0, 0, 5),
                        sample(1, 0, 0, 15),
                        sample(2, 0, 0, 25)));

        assertThat(summary.start()).isEqualTo(5);
        assertThat(summary.end()).isEqualTo(25);
        assertThat(summary.max()).isEqualTo(25);
        assertThat(summary.slopePerSecond()).isEqualTo(10.0);
    }

    private static HttpMatchedTradeCompletionLoadGenerator.SteadySample sample(
            double elapsedSeconds,
            long queueBacklog,
            long queueReadFailures) {
        return sample(elapsedSeconds, queueBacklog, queueReadFailures, 0);
    }

    private static HttpMatchedTradeCompletionLoadGenerator.SteadySample sample(
            double elapsedSeconds,
            long queueBacklog,
            long queueReadFailures,
            long orderReservationInboxBacklog) {
        return new HttpMatchedTradeCompletionLoadGenerator.SteadySample(
                elapsedSeconds, 0, 0, 0, 0, 0, 0,
                queueBacklog, queueReadFailures, orderReservationInboxBacklog);
    }
}
