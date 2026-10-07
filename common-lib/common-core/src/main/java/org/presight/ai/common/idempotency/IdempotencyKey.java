package org.presight.ai.common.idempotency;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Key under which the outcome of a mutating operation is recorded, so a replay returns
 * the original result instead of performing the operation twice.
 *
 * <p>Two sources, never mixed:
 * <ul>
 *   <li>{@link #of(String)} — a client-supplied {@code Idempotency-Key} header (public API).</li>
 *   <li>{@link #derive(String, String, String)} — server-derived for AI tool calls.
 *       The model never supplies the key, so a hallucinated duplicate call collapses
 *       onto the same key and yields exactly one effect.</li>
 * </ul>
 */
public record IdempotencyKey(String value) {

    public static final int MAX_LENGTH = 128;
    private static final Pattern ALLOWED = Pattern.compile("[A-Za-z0-9._:-]+");

    public IdempotencyKey {
        Objects.requireNonNull(value, "value");
        if (value.isEmpty() || value.length() > MAX_LENGTH || !ALLOWED.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "Idempotency key must be 1-" + MAX_LENGTH + " chars of [A-Za-z0-9._:-]");
        }
    }

    public static IdempotencyKey of(String clientSupplied) {
        return new IdempotencyKey(clientSupplied);
    }

    /**
     * {@code sha256(userId, subjectId, operation)} with length-prefixed fields, so that
     * {@code ("ab","c")} and {@code ("a","bc")} cannot collide.
     *
     * @param userId    from the security context — never from model output
     * @param subjectId the server-issued handle being acted on, e.g. a {@code holdId}
     * @param operation e.g. {@code confirmBooking}
     */
    public static IdempotencyKey derive(String userId, String subjectId, String operation) {
        MessageDigest sha256;
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is mandatory on every JVM", e);
        }
        for (String part : new String[] {userId, subjectId, operation}) {
            if (part == null || part.isBlank()) {
                throw new IllegalArgumentException("userId, subjectId and operation are all required");
            }
            byte[] bytes = part.getBytes(StandardCharsets.UTF_8);
            sha256.update(Integer.toString(bytes.length).getBytes(StandardCharsets.US_ASCII));
            sha256.update((byte) ':');
            sha256.update(bytes);
        }
        return new IdempotencyKey("d:" + HexFormat.of().formatHex(sha256.digest()));
    }
}
