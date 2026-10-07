package org.presight.ai.common.web;

import java.net.URI;
import java.util.Locale;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.error.ErrorCategory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler;
import reactor.core.publisher.Mono;

/**
 * Renders every error as RFC 9457 {@code application/problem+json}.
 *
 * <p>Framework exceptions (validation, 404 routing, 415, …) are handled by the inherited
 * {@link ResponseEntityExceptionHandler}; {@link DomainException}s add a stable
 * {@code code} property that clients — including AI tool callbacks — branch on.
 * Unexpected exceptions are deliberately not handled here, so Boot's default handler
 * returns a bare 500 without leaking internals.
 */
@RestControllerAdvice
public class ProblemDetailsExceptionHandler extends ResponseEntityExceptionHandler {

    static final String TYPE_PREFIX = "urn:problem:train-booking:";
    public static final String CODE_PROPERTY = "code";

    @ExceptionHandler(DomainException.class)
    public Mono<ResponseEntity<ProblemDetail>> handleDomain(DomainException ex) {
        HttpStatus status = statusOf(ex.category());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, ex.getMessage());
        problem.setType(URI.create(TYPE_PREFIX + ex.code().toLowerCase(Locale.ROOT).replace('_', '-')));
        problem.setProperty(CODE_PROPERTY, ex.code());
        return Mono.just(ResponseEntity.status(status).body(problem));
    }

    static HttpStatus statusOf(ErrorCategory category) {
        return switch (category) {
            case INVALID -> HttpStatus.UNPROCESSABLE_CONTENT;
            case NOT_FOUND -> HttpStatus.NOT_FOUND;
            case CONFLICT -> HttpStatus.CONFLICT;
            case FORBIDDEN -> HttpStatus.FORBIDDEN;
            case UNAVAILABLE -> HttpStatus.SERVICE_UNAVAILABLE;
        };
    }
}
