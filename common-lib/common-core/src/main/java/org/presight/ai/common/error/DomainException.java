package org.presight.ai.common.error;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Base for expected business failures. {@code code} is a stable machine-readable
 * identifier (e.g. {@code SEAT_ALREADY_HELD}) that clients — including the AI layer —
 * branch on; the message is for humans and may change.
 */
public class DomainException extends RuntimeException {

    private static final Pattern CODE_FORMAT = Pattern.compile("[A-Z][A-Z0-9_]*");

    private final ErrorCategory category;
    private final String code;

    public DomainException(ErrorCategory category, String code, String message) {
        this(category, code, message, null);
    }

    public DomainException(ErrorCategory category, String code, String message, Throwable cause) {
        super(message, cause);
        this.category = Objects.requireNonNull(category, "category");
        if (code == null || !CODE_FORMAT.matcher(code).matches()) {
            throw new IllegalArgumentException("Error code must be UPPER_SNAKE_CASE: " + code);
        }
        this.code = code;
    }

    public ErrorCategory category() {
        return category;
    }

    public String code() {
        return code;
    }

    public static DomainException notFound(String code, String message) {
        return new DomainException(ErrorCategory.NOT_FOUND, code, message);
    }

    public static DomainException conflict(String code, String message) {
        return new DomainException(ErrorCategory.CONFLICT, code, message);
    }

    public static DomainException invalid(String code, String message) {
        return new DomainException(ErrorCategory.INVALID, code, message);
    }
}
