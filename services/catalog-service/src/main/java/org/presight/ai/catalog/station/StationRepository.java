package org.presight.ai.catalog.station;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

public interface StationRepository extends ReactiveMongoRepository<Station, String> {
}
