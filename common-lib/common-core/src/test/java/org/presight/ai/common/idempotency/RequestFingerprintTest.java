package org.presight.ai.common.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RequestFingerprintTest {

    @Test
    void stableForEqualInput() {
        assertThat(RequestFingerprint.of("ER101", 3, null)).isEqualTo(RequestFingerprint.of("ER101", 3, null));
    }

    @Test
    void boundariesAndNullsAreDistinct() {
        assertThat(RequestFingerprint.of("ab", "c")).isNotEqualTo(RequestFingerprint.of("a", "bc"));
        assertThat(RequestFingerprint.of((Object) null)).isNotEqualTo(RequestFingerprint.of(""));
    }
}
