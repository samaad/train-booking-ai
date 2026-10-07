package org.presight.ai.assistant.fallback;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.presight.ai.assistant.client.ServiceClients.CatalogClient;
import org.presight.ai.assistant.client.ServiceClients.Station;
import org.presight.ai.assistant.config.AssistantProperties;
import org.presight.ai.assistant.event.ChatEvent;
import org.presight.ai.assistant.security.Caller;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * What chat does when the model is unavailable (02-ai-layer fallback tiers). Booking never depends
 * on the LLM: this answers a recognisable search deterministically, with real data and no prose.
 *
 * <ul>
 *   <li>Tier 3 — "Dubai to Fujairah tomorrow" style requests → catalog search results.</li>
 *   <li>Tier 4 — anything else → point the user to the regular booking form.</li>
 * </ul>
 */
@Component
public class DeterministicFallback {

    private static final Pattern ROUTE = Pattern.compile(
            "(?:from\\s+)?([\\p{L} ]+?)\\s+to\\s+([\\p{L} ]+?)(?:\\s+(?:on\\s+)?(today|tomorrow|\\d{4}-\\d{2}-\\d{2}))?\\s*[.?!]*$",
            Pattern.CASE_INSENSITIVE);
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    private final CatalogClient catalog;
    private final AssistantProperties properties;
    private final MeterRegistry meters;

    DeterministicFallback(CatalogClient catalog, AssistantProperties properties, MeterRegistry meters) {
        this.catalog = catalog;
        this.properties = properties;
        this.meters = meters;
    }

    record Intent(String from, String to, LocalDate date) {
    }

    public Mono<ChatEvent.Fallback> answer(Caller caller, String message, String reason) {
        return catalog.stations(caller.authorization()).collectList()
                .timeout(TIMEOUT)
                .flatMap(stations -> parse(message, stations)
                        .map(intent -> catalog.search(caller.authorization(), intent.from(), intent.to(), intent.date())
                                .collectList()
                                .timeout(TIMEOUT)
                                .map(results -> tier(3, "The assistant is unavailable (" + reason
                                        + "). Here are the trains from " + intent.from() + " to " + intent.to()
                                        + " on " + intent.date() + ".", results)))
                        .orElseGet(() -> Mono.just(formFallback(reason))))
                .onErrorResume(e -> Mono.just(formFallback(reason)));
    }

    private ChatEvent.Fallback formFallback(String reason) {
        return tier(4, "The assistant is unavailable (" + reason + "). Please use the booking form to search and book.", null);
    }

    private ChatEvent.Fallback tier(int tier, String message, Object results) {
        meters.counter("llm.fallback.triggered", "tier", Integer.toString(tier)).increment();
        return new ChatEvent.Fallback(tier, message, results);
    }

    Optional<Intent> parse(String message, List<Station> stations) {
        Matcher m = ROUTE.matcher(message.trim());
        if (!m.find()) {
            return Optional.empty();
        }
        Optional<String> from = resolve(m.group(1), stations); // "show trains from dubai" — the station ends the phrase
        Optional<String> to = resolve(m.group(2), stations);
        if (from.isEmpty() || to.isEmpty() || from.equals(to)) {
            return Optional.empty();
        }
        LocalDate today = LocalDate.now(properties.zone());
        String when = m.group(3) == null ? "today" : m.group(3).toLowerCase(Locale.ROOT);
        LocalDate date = switch (when) {
            case "today" -> today;
            case "tomorrow" -> today.plusDays(1);
            default -> LocalDate.parse(when);
        };
        return Optional.of(new Intent(from.get(), to.get(), date));
    }

    private static Optional<String> resolve(String phrase, List<Station> stations) {
        String p = phrase.trim().toLowerCase(Locale.ROOT);
        return stations.stream()
                .filter(s -> p.equals(s.code().toLowerCase(Locale.ROOT)) || p.endsWith(s.name().toLowerCase(Locale.ROOT))
                        || p.endsWith(s.city().toLowerCase(Locale.ROOT)) || p.endsWith(" " + s.code().toLowerCase(Locale.ROOT)))
                .map(Station::code)
                .findFirst();
    }
}
