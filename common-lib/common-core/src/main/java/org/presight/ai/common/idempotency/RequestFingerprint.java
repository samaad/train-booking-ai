package org.presight.ai.common.idempotency;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Hash of the semantically relevant parts of a mutating request, stored next to its
 * idempotency key. A replay with the same key but a different fingerprint is a client bug
 * (key reuse) and must be rejected — never silently answered with the first result.
 */
public final class RequestFingerprint {

    private static final String NULL_MARKER = "\u0000null";

    private RequestFingerprint() {
    }

    /** Length-prefixed, so field boundaries cannot shift; {@code null} is distinct from {@code ""}. */
    public static String of(Object... parts) {
        MessageDigest sha256;
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is mandatory on every JVM", e);
        }
        for (Object part : parts) {
            byte[] bytes = (part == null ? NULL_MARKER : part.toString()).getBytes(StandardCharsets.UTF_8);
            sha256.update(Integer.toString(bytes.length).getBytes(StandardCharsets.US_ASCII));
            sha256.update((byte) ':');
            sha256.update(bytes);
        }
        return HexFormat.of().formatHex(sha256.digest());
    }
}
