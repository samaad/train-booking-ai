package org.presight.ai.risk;

import java.time.Instant;

/**
 * One user's activity session. {@code signalled} is the highest level already reported, so a session that
 * keeps growing is flagged once per crossed threshold rather than on every event; {@code alert} marks the
 * update that crossed a new one.
 */
record Activity(long holds, long bookings, int signalled, boolean alert, Instant start, Instant end) {

    static final Activity EMPTY = new Activity(0, 0, 0, false, null, null);

    static Activity hold(Instant at) {
        return new Activity(1, 0, 0, false, at, at);
    }

    static Activity booking(Instant at) {
        return new Activity(0, 1, 0, false, at, at);
    }

    /** Folds one event into the session and decides whether this update raises a signal. */
    Activity add(Activity event, RiskRules rules) {
        long h = holds + event.holds;
        long b = bookings + event.bookings;
        int level = rules.level(h, b);
        return new Activity(h, b, Math.max(signalled, level), level > signalled, min(start, event.start), max(end, event.end));
    }

    /**
     * Two sessions joined by an event in the gap between them. Never an alert by itself: the event that bridged
     * them is folded in right afterwards, and that update decides.
     */
    Activity merge(Activity other) {
        return new Activity(holds + other.holds, bookings + other.bookings, Math.max(signalled, other.signalled), false,
                min(start, other.start), max(end, other.end));
    }

    private static Instant min(Instant a, Instant b) {
        return a == null ? b : b == null ? a : a.isBefore(b) ? a : b;
    }

    private static Instant max(Instant a, Instant b) {
        return a == null ? b : b == null ? a : a.isAfter(b) ? a : b;
    }
}
