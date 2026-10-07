package org.presight.ai.booking.booking;

import java.security.SecureRandom;

/** Six-character passenger name record, from an alphabet without look-alikes (no 0/O, 1/I). */
final class Pnr {

    private static final char[] ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789".toCharArray();
    private static final SecureRandom RANDOM = new SecureRandom();
    static final int LENGTH = 6;

    private Pnr() {
    }

    static String next() {
        char[] pnr = new char[LENGTH];
        for (int i = 0; i < LENGTH; i++) {
            pnr[i] = ALPHABET[RANDOM.nextInt(ALPHABET.length)];
        }
        return new String(pnr);
    }
}
