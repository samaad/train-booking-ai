package org.presight.ai.common.web;

import java.util.Locale;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.error.ErrorCategory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

/**
 * Translates failures of an inter-service {@code @HttpExchange} call into {@link DomainException}s,
 * for use with {@code onErrorMap}. Never retries: callers decide, and mutating calls must not.
 *
 * <ul>
 *   <li>Downstream 4xx → keeps the downstream problem {@code code}; a 404 becomes INVALID (the caller
 *       referenced something that does not exist), a 409 stays CONFLICT, other 4xx become INVALID.</li>
 *   <li>5xx, timeouts, refused connections → UNAVAILABLE with code {@code <SERVICE>_UNAVAILABLE}.</li>
 * </ul>
 */
public final class RemoteCallErrors {

    private RemoteCallErrors() {
    }

    public static Function<Throwable, Throwable> from(String service) {
        String prefix = service.toUpperCase(Locale.ROOT).replace('-', '_');
        return e -> switch (e) {
            case DomainException d -> d;
            case WebClientResponseException r when r.getStatusCode().is4xxClientError() -> rejected(prefix, r);
            case WebClientResponseException r -> unavailable(prefix, service, r);
            case WebClientRequestException r -> unavailable(prefix, service, r);
            case TimeoutException t -> unavailable(prefix, service, t);
            default -> e;
        };
    }

    private static DomainException rejected(String prefix, WebClientResponseException r) {
        ProblemDetail problem = problemOf(r);
        String code = problem != null && problem.getProperties() != null
                && problem.getProperties().get(ProblemDetailsExceptionHandler.CODE_PROPERTY) instanceof String c
                ? c : prefix + "_REJECTED";
        String detail = problem != null && problem.getDetail() != null ? problem.getDetail() : r.getStatusText();
        ErrorCategory category = r.getStatusCode().value() == HttpStatus.CONFLICT.value()
                ? ErrorCategory.CONFLICT : ErrorCategory.INVALID;
        return new DomainException(category, code, detail, r);
    }

    private static DomainException unavailable(String prefix, String service, Throwable cause) {
        return new DomainException(ErrorCategory.UNAVAILABLE, prefix + "_UNAVAILABLE",
                service + " is unavailable", cause);
    }

    private static ProblemDetail problemOf(WebClientResponseException r) {
        try {
            return r.getResponseBodyAs(ProblemDetail.class);
        } catch (RuntimeException notAProblem) {
            return null;
        }
    }
}
