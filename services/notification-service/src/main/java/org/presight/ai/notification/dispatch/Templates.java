package org.presight.ai.notification.dispatch;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

/** Message text per source event type. Placeholders are {@code {name}}; an unknown one is a bug, not blank text. */
@Component
class Templates {

    record Template(String subject, String body) {
    }

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{([a-zA-Z]+)}");

    private static final Map<String, Template> BY_EVENT = Map.of(
            "booking.confirmed.v1", new Template("Booking {pnr} confirmed",
                    "Your booking {pnr} on {trainNumber} ({from} to {to}) on {travelDate} is confirmed. Seat {seatNo}."),
            "booking.cancelled.v1", new Template("Booking {pnr} cancelled",
                    "Your booking {pnr} has been cancelled. Any payment will be refunded."),
            "payment.refunded.v1", new Template("Refund for {pnr}",
                    "We have refunded {amount} {currency} for booking {pnr}."),
            "payment.failed.v1", new Template("Payment for {pnr} failed",
                    "We could not take payment for booking {pnr}, so it has been cancelled and the seat released."),
            "ticket.issued.v1", new Template("Your ticket for {pnr}",
                    "Your ticket for booking {pnr} is ready. Show the QR code at the gate."));

    boolean supports(String eventType) {
        return BY_EVENT.containsKey(eventType);
    }

    Template render(String eventType, Map<String, String> params) {
        Template t = BY_EVENT.get(eventType);
        if (t == null) {
            throw new IllegalArgumentException("No template for " + eventType);
        }
        return new Template(fill(t.subject(), params), fill(t.body(), params));
    }

    private static String fill(String text, Map<String, String> params) {
        Matcher m = PLACEHOLDER.matcher(text);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String value = params.get(m.group(1));
            if (value == null) {
                throw new IllegalArgumentException("Missing template parameter: " + m.group(1));
            }
            m.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        return m.appendTail(out).toString();
    }
}
