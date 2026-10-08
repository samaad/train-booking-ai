package org.presight.ai.assistant.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class PriceGuardTest {

    private static final List<String> TOOLS = List.of("{\"amount\":\"142.00\",\"currency\":\"AED\",\"distanceKm\":130}");

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
    void digitsInsideIdsAndDatesNeverVouchForAPrice() {
        List<String> results = List.of("{\"bookingId\":\"0a99b3c4-1234-7abc-8def-000000000099\",\"travelDate\":\"2026-10-20\",\"amount\":\"142.00\"}");
        assertThat(PriceGuard.unsupportedAmounts("That will be AED 99.00.", results)).containsExactly("AED 99.00");
        assertThat(PriceGuard.unsupportedAmounts("That will be AED 20.", results)).containsExactly("AED 20");
        // A distance is not a price either.
        assertThat(PriceGuard.unsupportedAmounts("Only AED 130!", TOOLS)).containsExactly("AED 130");
    }

    @Test
    void plainNumbersAreNotPrices() {
        assertThat(PriceGuard.unsupportedAmounts("Train ER101 leaves at 07:55 from platform 3.", TOOLS)).isEmpty();
    }
}
