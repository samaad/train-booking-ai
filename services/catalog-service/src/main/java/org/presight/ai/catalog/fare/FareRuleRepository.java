package org.presight.ai.catalog.fare;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;

public interface FareRuleRepository extends ReactiveMongoRepository<FareRule, String> {

    Flux<FareRule> findByTrainNumber(String trainNumber);

    Flux<FareRule> findByTrainNumberAndFareClass(String trainNumber, String fareClass);
}
