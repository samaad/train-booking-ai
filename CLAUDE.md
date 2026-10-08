# train-booking-ai

AI-driven train booking system: reactive Spring microservices with a **local** Ollama model (`gemma4`) that books real trains by calling real services.

**Status:** Phases 0–6 built; Phase 7 Helm charts authored (hardening items not yet). Events (Phase 3) and security (Phase 4: Keycloak realm, PKCE clients, JWT resource server + ownership in every service, service-to-service client credentials, Kong CORS/limits/header stripping) are verified end to end in compose. Phase 6 (Kafka Streams seat availability, OTel tracing to Tempo across the AI seam and Kafka, custom metrics, Grafana dashboards, ECS logs) verified in compose. Not yet: React/mobile frontends, Phase 7 hardening (seat-contention load test, DLT replay tooling, fraud/velocity topology, Debezium CDC, batch-admin-service), `catalog.updated` producer, `ai.toolcall.v1`. Full plan in [`.claude/plan/`](.claude/plan/README.md); roadmap in [09-roadmap.md](.claude/plan/09-roadmap.md).

## Stack

Spring Boot 4.1.1 · Java 25 (GraalVM CE 25.0.2) · Spring Cloud 2025.1.3 · Spring AI 2.0.1 · WebFlux + R2DBC · Postgres/pgvector · MongoDB · Redis · Kafka · Keycloak 26.7 · Kong OSS 3.16 · Prometheus/Grafana/Tempo

Build with `./mvnw` — **there is no system `mvn`** on this machine.

## Hard constraints — do not violate

These were verified against Maven Central and bytecode, and are costly to rediscover.

### Versions

- **Spring AI must be 2.0.1, never 1.x.** The 1.1.x line targets Boot 3.5.7; only 2.x targets Boot 4.1. Mixing them fails at runtime in confusing ways.
- `search.maven.org`'s index is **stale** (reports 1.0.0 as newest). Trust `repo1.maven.org/.../maven-metadata.xml` instead.
- Starter names use 1.0+ convention: `spring-ai-starter-model-ollama`, `spring-ai-starter-vector-store-pgvector`. The old `spring-ai-ollama-spring-boot-starter` form does not exist in 2.x.
- Kong must be a **patched 3.16.x** — CVE-2026-14916 is a JWT algorithm-confusion auth bypass.
- **Kong publishes no OSS images after 3.9.x** (`library/kong` tops out at 3.9.3). 3.10+ ships only as `kong/kong-gateway`, which runs in free mode without a license — that is what compose uses. Never enable Kong's `jwt` plugin.
- **Boot 4 split WebClient out of `spring-boot-starter-webflux`** — add `spring-boot-starter-webclient`. `@HttpExchange` clients need no custom builders: `@ImportHttpServices(group=…)` + `spring.http.serviceclient.<group>.base-url`, timeouts under `spring.http.clients.*`.
- `@AutoConfigureWebTestClient` lives in `org.springframework.boot.webtestclient.autoconfigure`.
- **`@HttpExchange` `LocalDate` params need `@DateTimeFormat(iso = DATE)`** — otherwise they are sent in a locale format (`17/10/2026`) and the callee 400s. Test stubs must validate query params or this slips through.
- Boot 4 Mongo: connection is `spring.mongodb.uri`, but `spring.data.mongodb.representation.big-decimal` (set `decimal128`). Spring Data does not detect repository interfaces nested in another class.
- Flyway (`spring-boot-starter-flyway`) with R2DBC: set `spring.flyway.url/user/password`; no Hikari pool is created. Postgres 18 has native `uuidv7()`.
- R2DBC typed nulls: `io.r2dbc.spi.Parameters.in(R2dbcType.VARCHAR, value)` — not a `Class`.
- Testcontainers 2.x: `org.testcontainers.postgresql.PostgreSQLContainer`, `org.testcontainers.mongodb.MongoDBContainer`; R2DBC service connections need `testcontainers-r2dbc`. Spring Security `mockJwt()` only works in a MOCK web env — services test real RS256 tokens against a stub JWKS instead.
- MinIO publishes no community images any more; the object store is `adobe/s3mock` locally (S3 API, AWS SDK v2 async client). Exclude `apache-client`/`apache5-client`/`url-connection-client` from `software.amazon.awssdk:s3`.
- Jackson is **3.x** (`tools.jackson.*`). Decoding normalises `OffsetDateTime` to UTC — convert to the railway zone before showing times to the LLM, or it narrates "03:55" for a 07:55 departure.
- **Never call `UUID.randomUUID()` / default `SecureRandom` on an event loop**: NativePRNG reads `/dev/urandom` (blocking file I/O). `UuidV7` uses DRBG for this reason.

### Security (common-security) — conventions

- Every service builds its chain with `ResourceServerSecurity.build(http, rules)`; scopes/roles go on handlers as `@PreAuthorize` (e.g. `SCOPE_booking:write`, `hasRole('SERVICE')`). Keycloak roles map from `realm_access.roles` → `ROLE_*`, scopes → `SCOPE_*` (`KeycloakAuthorities`). Tokens must have `aud=train-api`.
- **Ownership**: records carry `user_id` (JWT `sub`); a non-owner gets **404, never 403** (`CallerIdentity.mayAccess`, staff = AGENT/SUPPORT/ADMIN). The owner travels in `booking.created/confirmed` so payment and ticketing enforce it without calling booking. Include the caller in idempotency fingerprints.
- **Relay the user's token** when a user is behind a call; use `ServiceTokens.call(...)` (client credentials, role SERVICE) only when nobody is — e.g. the saga's seat commit. `call` renews and retries once on 401.
- Realm: edit `infra/keycloak/build-realm.sh` and re-run it (it exports `realm-export.json`) — never hand-edit the JSON. Dev users aisha/omar/agent/admin, password `Passw0rd-dev`. Issuer is always `http://localhost:8180/realms/train-booking` (`KC_HOSTNAME`); services fetch JWKS/tokens via `keycloak:8080`.
- Keycloak 26 drops undeclared user attributes (declare them in the user profile); its session cookies are `Secure`+`SameSite=None` (fine for browsers on localhost, not for naive scripts).
- **Disable Spring Cloud's implicit HTTP-client circuit breakers** (`spring.cloud.circuitbreaker.reactive-http-services.enabled: false`) wherever the resilience4j starter is present: it hides real errors as `NoFallbackAvailableException` and lets callers' 4xx trip breakers for everyone.
- Flyway: services set `out-of-order: true` — the shared outbox migration is V1000, so later per-service V3+ must apply after it.

### Events (common-kafka) — conventions

- Event contracts live in `common-kafka` (`org.presight.ai.common.kafka.events`), topic names/partitions in `Topics`. Value = JSON string, `eventId`/`eventType` in headers. Consumers ignore unknown fields; breaking changes need a `.v2` topic.
- **Every event is written with `Outbox.append(...)` inside the same R2DBC transaction as the state change.** Never `KafkaTemplate.send` from business code (only notification's informational `notification.sent`). Schema comes from Flyway location `classpath:db/outbox` (`V1000__…`) — add it to `spring.flyway.locations`.
- Consumers: `@KafkaListener` + `IdempotentHandler.handle(record, Type, effect)` — the effect and the `processed_events` row commit together. Listener threads may `block()`; never do that on Netty.
- Errors: `DomainException` other than UNAVAILABLE, JSON and `IllegalArgumentException` → straight to `<topic>.<group>.DLT`; everything else retried with exponential backoff (~15 s) first. Dead-lettering is logged at ERROR.
- **No ordering across topics.** booking.created vs booking.cancelled (payment) and booking.confirmed vs booking.cancelled (ticketing) are handled with tombstones — copy that pattern for any new cross-topic dependency.
- Saga transitions are conditional on the expected `saga_state`; a stale event changes nothing. booking.cancelled is the single compensation trigger (inventory releases even committed seats, payment refunds, ticketing voids).
- Topics are declared per service (`TopicDeclarations`) — the broker has auto-create off.
- MongoDB in Boot 4: set `spring.mongodb.representation.uuid: standard` before storing `UUID` ids.
- Don't run the full build while the compose `services` stack is up: Testcontainers starves (Ryuk timeouts, 16-min container starts).

### Observability and Streams (Phase 6) — conventions

- Every service does `spring.config.import: classpath:train-booking-observability.yml` (in common-core): histograms, sampling, `spring.reactor.context-propagation: auto`, and Kafka template/listener observations. Tracing is `spring-boot-starter-opentelemetry` → otel-collector (4318) → Tempo 2.10.8. The endpoint is set **only** via `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT`.
- Trace context crosses the outbox through its `headers` jsonb column (`TraceContext.capture()` / `within()`). Postgres services add `r2dbc-proxy` to get DB spans.
- common-web registers a `SpanExportingPredicate` that drops root-level `query` spans (background pollers otherwise flood Tempo). **An `ObservationPredicate` cannot do this**: r2dbc-proxy sets the observation's name (null at predicate time) and parent only after predicates run.
- Never name a counter `*.created`: Prometheus reserves the `_created` suffix and the series silently disappears (hence `seat.hold.placed`).
- Logs are ECS JSON (`LOGGING_STRUCTURED_FORMAT_CONSOLE=ecs`). Trace ids are flat top-level `traceId`/`spanId` keys.
- `seat-availability-streams` (`streams/`, servlet + virtual threads, `exactly_once_v2`) must declare its **source** topics as `NewTopic`s too, or startup dies with `MissingSourceTopicException`. Its output is the compacted `seat.availability.v1`, plus `GET /occupancy/{train}/{date}` and the WebSocket `/ws/seat-map`.
- AI `PriceGuard` only trusts money-named JSON fields (`amount|price|fare|total|cost`): digits inside UUIDs or dates must never vouch for a quoted price.

### Kubernetes (deploy/helm) — conventions

- Library chart `common` holds every template; service charts are `Chart.yaml` + `values.yaml` only. Run `deploy/helm/build.sh` (Docker-only helm 4 + kubeconform): **leaf charts must be re-vendored before the umbrella**, or edits to `common` silently render the old templates. `charts/` and `Chart.lock` are gitignored.
- Pods run read-only root FS, UID 10001 (pinned numerically in `service.Dockerfile` — `runAsNonRoot` cannot verify a user *name*), all caps dropped; `/tmp` and any extra writable dir must be an emptyDir (`writableDirs`). Verified by running compose under the same restrictions.
- Secrets only via `ExternalSecret` (`secretEnv` maps env var → property under `train-booking/<service>`). The gitignore excludes `deploy/helm/**/*-secret.yaml` — don't name templates that way.
- `seat-availability-streams` stays at 1 replica (`Recreate`) until interactive queries route via `application.server`. AI service scales on `ai_scheduler_queued`, not CPU. Ollama is reached as `http://ollama:11434` via the umbrella's ExternalName Service.

### Spring AI 2.0.1 — verified from sources

- Models no longer execute tools. **`ChatClient`'s `ToolCallingAdvisor` hard-codes `Schedulers.boundedElastic()`** for tool execution and `ChatModelStreamAdvisor` `publishOn(boundedElastic)` — incompatible with a bounded `aiScheduler`. So `OllamaChatOrchestrator` drives `ChatModel.stream` directly and calls `ToolCallingManager.executeToolCalls` itself on `aiScheduler`. Identity goes in `ToolContext`.
- Properties are flat: `spring.ai.ollama.chat.model`, `spring.ai.ollama.chat.temperature` (`options.*` is deprecated), `spring.ai.ollama.embedding.model`, `spring.ai.vectorstore.pgvector.{dimensions,index-type,distance-type,initialize-schema}`. Set `dimensions: 1024` or startup calls Ollama.
- Ollama embeddings go through a blocking `RestClient` — fine only on `vectorScheduler`.
- **Global `spring.http.clients.read-timeout` also applies to Spring AI's Ollama clients.** Keep it above the model timeout; put tight timeouts on `spring.http.serviceclient.<group>.read-timeout`.
- BlockHound: surefire needs `-XX:+AllowRedefinitionToAddDeleteMethods -XX:+EnableDynamicAgentLoading`. Tolerated calls live in `AllowedBlockingCalls` (test `META-INF/services`) — each must be justified there.
- gemma4 observed: correct tool calls on explicit requests; tends to ask clarifying questions before mutating tools; may emit no text after `confirmBooking`.

### The AI path is NOT fully reactive

Two verified facts, both immovable:

1. **`PgVectorStore` is JDBC-only.** The 2.0.1 starter pulls `spring-boot-starter-jdbc` directly. `ai-assistant-service` therefore runs **two Postgres pools** (R2DBC max 10, Hikari max 8).
2. **`ToolCallback.call()` returns a bare `String`** — no `Mono`/`Flux` overload ([spring-ai#4434](https://github.com/spring-projects/spring-ai/issues/4434)). Streaming *is* reactive (`Flux<ChatResponse>`); **the blocking boundary is exactly one seam: the tool callback.**

Consequences that must hold in code:

- **`aiScheduler` must be explicitly bounded (cap ~8), never `Schedulers.boundedElastic()`** — the default is ~60k threads and will OOM against a 9.6 GB model. Backpressure is the feature.
- All pgvector calls go through `Mono.fromCallable(...).subscribeOn(vectorScheduler)` — never inline.
- Tool callbacks must never run on a Netty event-loop thread.
- **Keep BlockHound enabled in tests** once the AI service exists.
- Wrap Spring AI behind own interfaces (`ChatOrchestrator`, `TrainToolRegistry`) — it is the least stable dependency in the stack.

### Security

- **Capture identity from `SecurityContext` EAGERLY, before the `subscribeOn` boundary.** Reactor Context does not automatically cross it; reading it lazily inside a tool callback yields empty, and any code treating that as "no restriction" is an **auth bypass**.
- **`userId` is never a model-supplied tool parameter.** Model supplies *intent*; security context supplies *identity*. These paths never cross.
- **Kong OSS cannot do real OIDC** (`openid-connect` is Enterprise-only; the OSS `jwt` plugin cannot rotate JWKS). Kong does routing/rate-limiting only; **authoritative JWT validation lives in Spring Security at each service.**
- Spring Security 7 is a deletion release: lambda DSL mandatory, `authorizeExchange()` not `authorizeRequests()`, `AuthorizationManager` not `AccessDecisionManager`. Old examples will not compile.
- Never use `keycloak-spring-security-adapter` — dead for years.

### Correctness

- **Postgres is the source of truth for seat holds; Redis is only a contention filter.** The conditional `UPDATE ... WHERE status='AVAILABLE' OR hold_expires_at < now()` must be correct with zero Redis involvement.
- **No blind retry on mutating calls** (seat holds, payments). Retrying without knowing whether it committed causes double-bookings and double-charges. Use idempotent replay with the original key.
- **No retry on Ollama** — a retried 120s call turns one slow request into three.
- Outbox delivery is **at-least-once**; every Kafka consumer must be idempotent.
- **The LLM never creates a booking.** `confirmBooking` requires a server-issued `holdId` plus explicit user confirmation in the UI.

### Data

- `mxbai-embed-large` has a **512-token context** and produces **1024-dim** vectors. Chunk RAG content to ≤380 tokens and **fail ingestion on any chunk >500** — oversized chunks are silently truncated, which degrades retrieval without ever erroring.
- Prefix RAG queries with `Represent this sentence for searching relevant passages:` (mxbai is trained asymmetrically).

### Kafka

- **Use `spring-kafka`, not `reactor-kafka`** — Reactor Kafka is discontinued; `ReactiveKafka*Template` and the Reactor Kafka binder are deprecated for removal.
- Kafka Streams is blocking by design. Isolate it; never call reactive code inside a `Processor`.

### Inter-service calls

- **Use `@HttpExchange` interface clients over `WebClient`**, registered via `@ImportHttpServices`. **Do not add `spring-cloud-starter-openfeign`** to any reactive service — Feign is blocking and Spring Cloud has declared it feature-complete. Feign is acceptable *only* in a future non-reactive batch/admin service.

## Local environment

- Ollama (now 0.35.0) runs **on the host**, not in Docker (macOS containers get no GPU). Reach it via `host.docker.internal:11434` from containers; needs `OLLAMA_HOST=0.0.0.0`.
- Models already pulled: `gemma4:latest` (9.6 GB, tool-capable), `mxbai-embed-large`.
- **36 GB RAM / 12 CPU.** gemma4 alone is ~9.6 GB — use Docker Compose **profiles**, never run `full` alongside several JVMs.
- **Docker Desktop is capped at ~8 GB** — `full` (~9 GB) does not fit. Every container has a `mem_limit`.
- Host port 5432 is taken by an unrelated `postgres_db` container — `infra/.env` (gitignored) sets `POSTGRES_PORT=5433`. Don't stop that container.
- Compose: **`./mvnw -q clean package -DskipTests`** (without `clean`, Boot repackage skips the up-to-date fat jars and images ship a stale common-lib), then `docker compose -f infra/docker-compose.yml --profile <service>|services up -d --build --wait`. A service profile pulls in the infra it needs; `services` starts all seven (~3.5 GB of JVMs). Images copy the host-built jar.
- Host port 8088 is the user's own ksqlDB — leave it alone. s3mock is on host 9190 (Prometheus has 9090).
- Run services from the IDE against compose infra with `DB_PORT=5433` (see the postgres_db note above).
- No kubectl context, helm or kind installed — K8s charts are authoring-only (lint/template/kubeconform via Docker), never installed here.

## Repo conventions

- Layout: `common-lib/*` (plain jars), `services/*` (bootable), `streams/*` (Kafka Streams apps, blocking, isolated), `deploy/helm/` (charts), `infra/` (compose, init scripts, Kong, Prometheus/Grafana).
- AI service: `POST /chat` (SSE events: conversation, token, tool, confirmation, warning, fallback, error, done), `GET /conversations`, `GET /conversations/{id}/messages`; Kong `/api/assistant`. Compose profile `assistant`.
- Ports: catalog 8081, inventory 8082, booking 8083, payment 8084, ticketing 8085, notification 8086, user-profile 8087, seat-availability-streams 8089, ai-assistant 8080. Prometheus 9090, Grafana 3000, Tempo 3200. Kong proxy 8000, Keycloak 8180, kafka-ui 8090.
- `common-core` stays framework-free (ids, `DomainException`, `IdempotencyKey`, `RequestFingerprint`). `common-web` auto-configures RFC 9457 problem+json error mapping — throw `DomainException` with an `UPPER_SNAKE` code rather than building error responses by hand. Use `IdempotencyKeyHeader` for the header and `RemoteCallErrors.from(service)` in `onErrorMap` on every inter-service call.
- Mutating endpoints take `Idempotency-Key`; store key + `RequestFingerprint` with a UNIQUE constraint; replay returns the original 201, a reused key with a different body is 422. State transitions are single conditional UPDATEs (`WHERE status = … AND version = …`), never read-modify-write.
- Kong routes: `/api/{catalog,inventory,booking,payment,ticketing,profile,assistant,availability}/…` (prefix stripped). notification has no public API.

- Maven multi-module monorepo; parent POM is `<packaging>pom</packaging>` with **no `src/`** (a parent POM cannot carry sources).
- `spring-boot-maven-plugin` belongs in `<pluginManagement>`, not `<plugins>` — otherwise `common-lib` is repackaged into a non-importable fat jar.
- groupId `org.presight.ai`.