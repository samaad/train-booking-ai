package org.presight.ai.assistant.support;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

/**
 * The complete list of blocking calls tolerated on non-blocking threads — each one-time and justified.
 * Anything not listed here fails the test suite.
 */
public class AllowedBlockingCalls implements BlockHoundIntegration {

    @Override
    public void applyTo(BlockHound.Builder builder) {
        // DRBG seeding reads OS entropy exactly once, when UuidV7 initialises; it is pure CPU afterwards.
        builder.allowBlockingCallsInside("org.presight.ai.common.id.UuidV7", "<clinit>");
        // The MongoDB reactive driver guards its connection-pool permits with a short, I/O-free
        // ReentrantLock section when a pooled connection is released on a callback thread.
        builder.allowBlockingCallsInside("com.mongodb.internal.Locks", "checkedWithLock");
        // The driver mints a server-session id with UUID.randomUUID() (NativePRNG reads /dev/urandom) when its
        // session pool grows. Pooled, so rare; and it is the driver's code, not ours.
        builder.allowBlockingCallsInside("com.mongodb.internal.session.ServerSessionPool", "createNewServerSessionIdentifier");
    }
}
