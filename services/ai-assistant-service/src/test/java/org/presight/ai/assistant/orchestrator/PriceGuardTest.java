package org.presight.ai.assistant.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PriceGuardTest {

    private static final String TOOLS = "{\"amount\":\"142.00\",\"currency\":\"AED\",\"distanceKm\":130}";

    @Test
    void amountsFromToolsPass() {
        assertThat(PriceGuard.unsupportedAmounts("The fare is AED 142.00, or 142 AED.", TOOLS)).isEmpty();
    }

    @Test
    void inventedAmountsAreReported() {
        assertThat(PriceGuard.unsupportedAmounts("Only AED 99.50! Or 120 dirhams.", TOOLS))
                .containsExactly("AED 99.50", "120 dirhams");
    }

    @Test
    void plainNumbersAreNotPrices() {
        assertThat(PriceGuard.unsupportedAmounts("Train ER101 leaves at 07:55 from platform 3.", TOOLS)).isEmpty();
    }
}
