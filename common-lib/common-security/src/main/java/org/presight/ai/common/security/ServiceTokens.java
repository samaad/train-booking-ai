package org.presight.ai.common.security;

import java.util.function.Function;
import org.springframework.security.oauth2.client.OAuth2AuthorizeRequest;
import org.springframework.security.oauth2.client.ReactiveOAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.ReactiveOAuth2AuthorizedClientService;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/**
 * This service's own identity for calls no user is behind (e.g. a saga step reacting to an event): an access
 * token from the {@code train-services} client-credentials grant, cached and renewed by Spring Security.
 * Where a user <em>is</em> behind a call, relay their token instead — downstream must authorise the user.
 */
public class ServiceTokens {

    public static final String REGISTRATION = "train-services";

    private final ReactiveOAuth2AuthorizedClientManager manager;
    private final ReactiveOAuth2AuthorizedClientService cache;
    private final String principal;

    public ServiceTokens(ReactiveOAuth2AuthorizedClientManager manager, ReactiveOAuth2AuthorizedClientService cache,
                         String principal) {
        this.manager = manager;
        this.cache = cache;
        this.principal = principal;
    }

    /**
     * Runs {@code call} with the service token. If downstream answers 401 — the cached token was revoked, or
     * its signing key retired before it expired — the token is evicted and the call retried once with a fresh
     * one. Safe even for mutating calls: a 401 means the call was not executed.
     */
    public <T> Mono<T> call(Function<String, Mono<T>> call) {
        return authorization().flatMap(call)
                .onErrorResume(WebClientResponseException.Unauthorized.class, unauthorized ->
                        cache.removeAuthorizedClient(REGISTRATION, principal)
                                .then(authorization())
                                .flatMap(call));
    }

    /** {@code "Bearer <token>"}, ready for an Authorization header. */
    public Mono<String> authorization() {
        return manager.authorize(OAuth2AuthorizeRequest.withClientRegistrationId(REGISTRATION).principal(principal).build())
                .switchIfEmpty(Mono.error(() -> new IllegalStateException("No service token from " + REGISTRATION)))
                .map(client -> "Bearer " + client.getAccessToken().getTokenValue());
    }
}
