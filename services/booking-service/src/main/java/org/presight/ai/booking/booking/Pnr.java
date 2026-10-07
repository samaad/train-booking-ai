package org.presight.ai.booking.booking;

import java.security.SecureRandom;

/** Six-character passenger name record, from an alphabet without look-alikes (no 0/O, 1/I). */
final class Pnr {

    private static final char[] ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789".toCharArray();
    private static final SecureRandom RANDOM = drbg(); // not NativePRNG: no /dev/urandom reads on the event loop
    static final int LENGTH = 6;

    private Pnr() {
    }

    private static SecureRandom drbg() {
        try {
            SecureRandom random = SecureRandom.getInstance("DRBG");
            random.nextBytes(new byte[8]);
            return random;
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("DRBG is mandatory since JDK 9", e);
        }
    }

    static String next() {
        char[] pnr = new char[LENGTH];
        for (int i = 0; i < LENGTH; i++) {
            pnr[i] = ALPHABET[RANDOM.nextInt(ALPHABET.length)];
        }
        return new String(pnr);
    }
}
