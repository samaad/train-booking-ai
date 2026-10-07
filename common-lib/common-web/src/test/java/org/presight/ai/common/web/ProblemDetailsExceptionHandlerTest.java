package org.presight.ai.common.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.presight.ai.common.error.DomainException;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

class ProblemDetailsExceptionHandlerTest {

    private WebTestClient client;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToController(new FailingController())
                .controllerAdvice(new ProblemDetailsExceptionHandler())
                .build();
    }

    @Test
    void conflictBecomes409ProblemWithStableCode() {
        client.get().uri("/seats/12A").exchange()
                .expectStatus().isEqualTo(409)
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.code").isEqualTo("SEAT_ALREADY_HELD")
                .jsonPath("$.type").isEqualTo("urn:problem:train-booking:seat-already-held")
                .jsonPath("$.detail").isEqualTo("Seat 12A is already held");
    }

    @Test
    void frameworkErrorsAreAlsoProblems() {
        client.get().uri("/seats/12A").accept(MediaType.APPLICATION_XML).exchange()
                .expectStatus().isEqualTo(406)
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
    }

    @RestController
    static class FailingController {
        @GetMapping(value = "/seats/{seat}", produces = MediaType.APPLICATION_JSON_VALUE)
        Mono<String> seat(@PathVariable String seat) {
            return Mono.error(DomainException.conflict("SEAT_ALREADY_HELD", "Seat " + seat + " is already held"));
        }
    }
}
