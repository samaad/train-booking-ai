package org.presight.ai.common.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IdempotencyKeyTest {

    @Test
    void derivationIsDeterministic() {
        assertThat(IdempotencyKey.derive("user-1", "hold-9", "confirmBooking"))
                .isEqualTo(IdempotencyKey.derive("user-1", "hold-9", "confirmBooking"));
    }

    @Test
    void derivationSeparatesUsersAndOperations() {
        IdempotencyKey base = IdempotencyKey.derive("user-1", "hold-9", "confirmBooking");

        assertThat(IdempotencyKey.derive("user-2", "hold-9", "confirmBooking")).isNotEqualTo(base);
        assertThat(IdempotencyKey.derive("user-1", "hold-9", "cancelBooking")).isNotEqualTo(base);
    }

    @Test
    void fieldBoundariesCannotBeShifted() {
        assertThat(IdempotencyKey.derive("ab", "c", "op"))
                .isNotEqualTo(IdempotencyKey.derive("a", "bc", "op"));
    }

    @Test
    void derivedKeyIsAValidKey() {
        String value = IdempotencyKey.derive("u", "h", "op").value();

        assertThat(IdempotencyKey.of(value).value()).isEqualTo(value);
    }

    @Test
    void derivationRequiresIdentity() {
        assertThatThrownBy(() -> IdempotencyKey.derive(" ", "hold-9", "confirmBooking"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "has space", "semi;colon", "ünïcode"})
    void rejectsMalformedClientKeys(String raw) {
        assertThatThrownBy(() -> IdempotencyKey.of(raw)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsOverlongClientKeys() {
        assertThatThrownBy(() -> IdempotencyKey.of("a".repeat(IdempotencyKey.MAX_LENGTH + 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsUuid() {
        assertThat(IdempotencyKey.of("018f3c2e-7b1a-7c00-8000-000000000000")).isNotNull();
    }
}
