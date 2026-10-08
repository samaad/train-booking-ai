package org.presight.ai.assistant.orchestrator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Guardrail 4 (02-ai-layer): every money figure in the model's answer must appear in this turn's tool
 * results. A mitigation, not a proof — the real protection is that the model cannot book anything.
 */
final class PriceGuard {

    private static final Pattern MONEY = Pattern.compile(
            "(?:(?:AED|USD|EUR|Dhs?\\.?|\\$|€)\\s?(\\d+(?:[.,]\\d{1,2})?))|(?:(\\d+(?:[.,]\\d{1,2})?)\\s?(?:AED|dirhams?|USD|EUR))",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DECIMAL = Pattern.compile("\\d+(?:\\.\\d+)?");
    private static final Pattern MONEY_FIELD = Pattern.compile(".*(amount|price|fare|total|cost).*");
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private PriceGuard() {
    }

    /**
     * Amounts quoted in {@code answer} that match no money value in the tool results (JSON documents). Only
     * money-named fields count ({@code amount}, {@code price}, {@code baseFare}, …) — never digits inside ids or
     * dates, nor distances or durations: a UUID containing "99" must not vouch for an invented "AED 99.00".
     */
    static List<String> unsupportedAmounts(String answer, List<String> toolResultsJson) {
        List<BigDecimal> known = new ArrayList<>();
        for (String result : toolResultsJson) {
            try {
                collect(JSON.readTree(result), null, known);
            } catch (RuntimeException notJson) {
                // a tool result that is not JSON vouches for nothing
            }
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

    private static void collect(JsonNode node, String field, List<BigDecimal> into) {
        boolean moneyField = field != null && MONEY_FIELD.matcher(field.toLowerCase(Locale.ROOT)).matches();
        if (node.isNumber() && moneyField) {
            into.add(node.decimalValue());
        } else if (node.isString() && moneyField && DECIMAL.matcher(node.asString().trim()).matches()) {
            into.add(new BigDecimal(node.asString().trim()));
        } else if (node.isObject()) {
            node.properties().forEach(e -> collect(e.getValue(), e.getKey(), into));
        } else if (node.isArray()) {
            node.forEach(child -> collect(child, field, into));
        }
    }
}
