package org.presight.ai.admin;

import org.presight.ai.common.error.DomainException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** RFC 9457 errors with the same {@code code} property common-web gives the reactive services. */
@RestControllerAdvice
class ProblemDetails {

    @ExceptionHandler(DomainException.class)
    ProblemDetail domain(DomainException e) {
        HttpStatus status = switch (e.category()) {
            case INVALID -> HttpStatus.UNPROCESSABLE_CONTENT;
            case NOT_FOUND -> HttpStatus.NOT_FOUND;
            case CONFLICT -> HttpStatus.CONFLICT;
            case FORBIDDEN -> HttpStatus.FORBIDDEN;
            case UNAVAILABLE -> HttpStatus.SERVICE_UNAVAILABLE;
        };
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, e.getMessage());
        problem.setProperty("code", e.code());
        return problem;
    }
}
