package org.presight.ai.booking.client;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.service.registry.HttpServiceGroup.ClientType;
import org.springframework.web.service.registry.ImportHttpServices;

/**
 * Declarative reactive clients (CLAUDE.md: {@code @HttpExchange} over WebClient, never OpenFeign).
 * Base URLs and timeouts come from {@code spring.http.serviceclient.<group>.*}. WEB_CLIENT is
 * explicit: an unspecified client type could resolve to the blocking RestClient.
 */
@Configuration(proxyBeanMethods = false)
@ImportHttpServices(group = "inventory", types = InventoryClient.class, clientType = ClientType.WEB_CLIENT)
@ImportHttpServices(group = "catalog", types = CatalogClient.class, clientType = ClientType.WEB_CLIENT)
class ClientsConfiguration {
}
