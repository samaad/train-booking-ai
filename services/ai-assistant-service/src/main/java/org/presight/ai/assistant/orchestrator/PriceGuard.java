package org.presight.ai.assistant.orchestrator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Guardrail 4 (02-ai-layer): every money figure in the model's answer must appear in this turn's tool
 * results. A mitigation, not a proof — the real protection is that the model cannot book anything.
 */
final class PriceGuard {

    private static final Pattern MONEY = Pattern.compile(
            "(?:(?:AED|USD|EUR|Dhs?\\.?|\\$|€)\\s?(\\d+(?:[.,]\\d{1,2})?))|(?:(\\d+(?:[.,]\\d{1,2})?)\\s?(?:AED|dirhams?|USD|EUR))",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern NUMBER = Pattern.compile("\\d+(?:\\.\\d+)?");

    private PriceGuard() {
    }

    /** Amounts quoted in {@code answer} that match no number in {@code toolResults}. */
    static List<String> unsupportedAmounts(String answer, String toolResults) {
        List<BigDecimal> known = new ArrayList<>();
        Matcher n = NUMBER.matcher(toolResults);
        while (n.find()) {
            known.add(new BigDecimal(n.group()));
        }
        List<String> unsupported = new ArrayList<>();
        Matcher m = MONEY.matcher(answer);
        while (m.find()) {
            String raw = (m.group(1) != null ? m.group(1) : m.group(2)).replace(',', '.');
            BigDecimal amount = new BigDecimal(raw);
            if (known.stream().noneMatch(k -> k.compareTo(amount) == 0)) {
                unsupported.add(m.group().trim());
            }
        }
        return unsupported;
    }
}
