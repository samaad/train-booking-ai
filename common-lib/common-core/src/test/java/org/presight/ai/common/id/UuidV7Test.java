package org.presight.ai.common.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UuidV7Test {

    @Test
    void hasVersion7AndIetfVariant() {
        UUID id = UuidV7.generate();

        assertThat(id.version()).isEqualTo(7);
        assertThat(id.variant()).isEqualTo(2);
    }

    @Test
    void embedsTimestamp() {
        long millis = Instant.parse("2026-10-06T12:00:00Z").toEpochMilli();

        assertThat(UuidV7.timestamp(UuidV7.generate(millis))).isEqualTo(Instant.ofEpochMilli(millis));
    }

    @Test
    void sortsByCreationTimeAcrossMilliseconds() {
        UUID earlier = UuidV7.generate(1_000L);
        UUID later = UuidV7.generate(1_001L);

        // String order is what Postgres' uuid comparison effectively matches for v7.
        assertThat(earlier.toString()).isLessThan(later.toString());
    }

    @Test
    void rejectsNonV7() {
        assertThatThrownBy(() -> UuidV7.timestamp(UUID.randomUUID()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
