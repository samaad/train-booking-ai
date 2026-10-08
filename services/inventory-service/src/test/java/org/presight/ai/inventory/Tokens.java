package org.presight.ai.inventory;

import org.presight.ai.common.security.testing.TestIssuer;

/** One test issuer for the module; each test class wires it with @DynamicPropertySource. */
final class Tokens {

    static final TestIssuer ISSUER = new TestIssuer();
    static final String PASSENGER = ISSUER.passenger("user-1");
    static final String SERVICE = ISSUER.service("service-account-train-services");

    private Tokens() {
    }
}
