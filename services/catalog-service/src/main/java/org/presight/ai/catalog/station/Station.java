package org.presight.ai.catalog.station;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/** @param code short station code, e.g. {@code AUH} — the natural key used everywhere else */
@Document("stations")
public record Station(@Id String code, String name, String city) {
}
