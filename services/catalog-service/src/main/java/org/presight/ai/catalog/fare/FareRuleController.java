package org.presight.ai.catalog.fare;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class FareRuleController {

    private final FareRuleRepository rules;

    public FareRuleController(FareRuleRepository rules) {
        this.rules = rules;
    }

    @GetMapping("/fares/rules")
    public Flux<FareRule> rules(@RequestParam String trainNumber, @RequestParam(required = false) String fareClass) {
        return fareClass == null
                ? rules.findByTrainNumber(trainNumber)
                : rules.findByTrainNumberAndFareClass(trainNumber, fareClass);
    }
}
