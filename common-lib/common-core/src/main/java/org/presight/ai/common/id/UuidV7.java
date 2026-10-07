package org.presight.ai.common.id;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.UUID;

/**
 * RFC 9562 version-7 UUIDs: 48-bit Unix-millisecond timestamp followed by random bits.
 *
 * <p>Time-ordered, so they index well as Postgres primary keys (no B-tree page splits
 * from random v4 inserts), and unguessable enough to serve as server-issued handles
 * such as {@code holdId}. JDK 25 has no built-in v7 factory.
 *
 * <p>Uses the DRBG {@link SecureRandom}, not the platform default: NativePRNG refills its buffer by
 * reading {@code /dev/urandom} — a blocking file read, on whatever thread asks, which in a WebFlux
 * service is an event-loop thread (BlockHound catches it). DRBG reads entropy once, when this class
 * initialises, and is pure CPU afterwards.
 */
public final class UuidV7 {

    private static final SecureRandom RANDOM = drbg();

    private UuidV7() {
    }

    public static UUID generate() {
        return generate(System.currentTimeMillis());
    }

    static UUID generate(long epochMillis) {
        byte[] bytes = new byte[16];
        RANDOM.nextBytes(bytes);

        bytes[0] = (byte) (epochMillis >>> 40);
        bytes[1] = (byte) (epochMillis >>> 32);
        bytes[2] = (byte) (epochMillis >>> 24);
        bytes[3] = (byte) (epochMillis >>> 16);
        bytes[4] = (byte) (epochMillis >>> 8);
        bytes[5] = (byte) epochMillis;
        bytes[6] = (byte) ((bytes[6] & 0x0F) | 0x70); // version 7
        bytes[8] = (byte) ((bytes[8] & 0x3F) | 0x80); // IETF variant

        long msb = 0;
        long lsb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (bytes[i] & 0xFF);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (bytes[i] & 0xFF);
        }
        return new UUID(msb, lsb);
    }

    /** A seeded DRBG; the one-time seed read happens here, during class initialisation. */
    static SecureRandom drbg() {
        try {
            SecureRandom random = SecureRandom.getInstance("DRBG");
            random.nextBytes(new byte[16]); // force seeding now, not on the first request
            return random;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("DRBG is mandatory since JDK 9", e);
        }
    }

    /** Extracts the embedded creation time. Rejects non-v7 ids rather than returning garbage. */
    public static Instant timestamp(UUID uuid) {
        if (uuid.version() != 7) {
            throw new IllegalArgumentException("Not a UUIDv7: " + uuid);
        }
        return Instant.ofEpochMilli(uuid.getMostSignificantBits() >>> 16);
    }
}
