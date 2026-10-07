package org.presight.ai.inventory;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/** CLAUDE.md: the conditional UPDATE must be correct with zero Redis involvement. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "inventory.contention-filter.enabled=false")
@Import(TestcontainersConfiguration.class)
class ConcurrentHoldPostgresOnlyTest extends ConcurrentHoldContract {
}
