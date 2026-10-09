package org.presight.ai.admin.dlt;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.List;
import org.presight.ai.admin.dlt.DeadLetters.DeadLetter;
import org.presight.ai.admin.dlt.DeadLetters.DltSummary;
import org.presight.ai.admin.dlt.DeadLetters.ReplayResult;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Dead-letter operations, ADMIN only (SecurityConfiguration). Every replay is logged with the admin's subject. */
@RestController
@RequestMapping("/dlt")
@Validated
class DltController {

    private final DeadLetters deadLetters;

    DltController(DeadLetters deadLetters) {
        this.deadLetters = deadLetters;
    }

    @GetMapping
    List<DltSummary> summaries() {
        return deadLetters.summaries();
    }

    @GetMapping("/{topic}/messages")
    List<DeadLetter> browse(@PathVariable String topic,
                            @RequestParam(required = false) Integer partition,
                            @RequestParam(required = false) @Min(0) Long fromOffset,
                            @RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit) {
        return deadLetters.browse(topic, partition, fromOffset, limit);
    }

    @PostMapping("/{topic}/messages/{partition}/{offset}/replay")
    DeadLetter replayOne(@AuthenticationPrincipal Jwt jwt, @PathVariable String topic, @PathVariable int partition,
                         @PathVariable long offset) {
        return deadLetters.replayOne(topic, partition, offset, jwt.getSubject());
    }

    @PostMapping("/{topic}/replay")
    ReplayResult replayPending(@AuthenticationPrincipal Jwt jwt, @PathVariable String topic,
                               @RequestParam(defaultValue = "100") @Min(1) @Max(10_000) int max) {
        return deadLetters.replayPending(topic, max, jwt.getSubject());
    }
}
