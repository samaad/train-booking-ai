# 08 — Local Dev, Deploy & Repo Layout

## The 36 GB problem

`gemma4` alone is ~9.6 GB resident. Full infra plus 8 JVMs will not fit comfortably. **Docker Compose profiles are mandatory, not a nicety.**

| Profile | Contents | Rough RAM |
|---|---|---|
| `core` | Postgres (pgvector image), Redis | ~1.5 GB |
| `mongo` | MongoDB | ~1 GB |
| `kafka` | Kafka (KRaft, no ZooKeeper), kafka-ui | ~2.5 GB |
| `auth` | Keycloak + its Postgres | ~1.5 GB |
| `gateway` | Kong OSS (DB-less declarative) | ~0.5 GB |
| `observability` | Prometheus, Grafana, Tempo, OTel collector | ~1.5 GB |
| `full` | everything | ~9 GB + JVMs |

**Run `core` plus the one slice under development.** Never run `full` while also running several services from the IDE.

## Reaching Ollama

> **Ollama stays on the host — do not containerise it.** Docker Desktop on macOS cannot pass through the Apple GPU, so a containerised Ollama runs CPU-only: a drastic slowdown for a 9.6 GB model. Keep it native for Metal acceleration.

| Environment | Base URL |
|---|---|
| Service on host (IDE) | `http://localhost:11434` |
| Service in Docker (macOS/Windows) | `http://host.docker.internal:11434` |
| Service in Docker (Linux) | `--add-host=host.docker.internal:host-gateway` — **not automatic on Linux** |
| K8s local (kind/minikube) | `ExternalName` Service → host (keeps app config identical) |
| K8s cluster | `http://ollama.ai.svc.cluster.local:11434` (StatefulSet, GPU node pool, PVC for models) |

Externalise as **`SPRING_AI_OLLAMA_BASE_URL`** so one image runs everywhere.

> Ollama binds `127.0.0.1` by default. Reaching it from containers requires **`OLLAMA_HOST=0.0.0.0`** on the host.

## Kubernetes

> **No kubectl context is configured on this machine** (`kubectl` present, no cluster). K8s manifests are an **authoring exercise, not a deploy target** — write them, don't claim they're tested.

Structure: Helm umbrella chart + per-service subcharts, with a **`common` library chart** holding shared Deployment/Service/HPA/ServiceMonitor templates so each service subchart supplies values only. Avoids 8× template duplication.

Per service: Deployment (2 replicas), Service, HPA (CPU + custom Prometheus metric), PDB, `ServiceMonitor`, `ConfigMap`, `ExternalSecret`. Probes on `/actuator/health/liveness` and `/readiness`.

`ai-assistant-service` needs its own resource profile — high memory, low replica count, and realistically a GPU node selector in any real deployment.

### GraalVM native: don't

Despite GraalVM CE 25 being installed, native builds are slow (minutes), complicate reflection/AOT config (Lombok + Spring AI), and native support for Spring AI 2.0.1 is unproven. **JVM mode throughout**; revisit only for stable, low-traffic services after Phase 7. Don't spend early budget on it.

## Monorepo layout

```
train-booking-ai/
├── pom.xml                          # parent: <packaging>pom</packaging>
├── common-lib/
│   ├── common-core/                 # DTOs, events, errors, idempotency, UUIDv7
│   ├── common-security/             # reactive JWT converter, principal propagation
│   ├── common-kafka/                # envelope, serdes, topic names, outbox relay, idempotent consumer
│   ├── common-observability/        # Micrometer + tracing config
│   ├── common-resilience/           # Resilience4j presets
│   └── common-web/                  # WebClient builders, @HttpExchange registration, error mapping
├── services/
│   ├── catalog-service/
│   ├── inventory-service/
│   ├── booking-service/
│   ├── payment-service/
│   ├── ticketing-service/
│   ├── notification-service/
│   ├── user-profile-service/
│   └── ai-assistant-service/
├── streams/
│   └── seat-availability-streams/
├── contracts/                       # OpenAPI + event schemas
├── frontend/
│   ├── web/                         # React + Vite + TS, oidc-client-ts (PKCE)
│   └── mobile/                      # React Native / Expo
├── infra/
│   ├── docker-compose.yml           # profile-gated
│   ├── postgres/init/               # multi-DB + CREATE EXTENSION vector
│   ├── keycloak/realm-export.json
│   ├── kong/kong.yml                # DB-less declarative
│   ├── observability/               # prometheus, grafana, tempo
│   └── rag-corpus/                  # markdown source docs for ingestion
├── deploy/helm/
└── docs/adr/
```

## Parent POM conversion — the gotchas

- `<packaging>pom</packaging>`, list modules, `dependencyManagement` importing **`spring-ai-bom:2.0.1`**. No milestone repo needed.
- **The root `src/` directory must be deleted.** A `packaging=pom` parent cannot carry sources; the build fails confusingly if it lingers.
- **`spring-boot-maven-plugin` goes in `<pluginManagement>`, not `<plugins>`** — otherwise `common-lib` gets repackaged into a non-importable fat jar.
- `TrainBookingAiApplication.java` relocates into whichever service is built first (`catalog-service` in Phase 1).
- `application.properties` splits per service; carries `SPRING_AI_OLLAMA_BASE_URL`, R2DBC, Kafka, Keycloak issuer config.
- `.gitignore` needs `node_modules/`, `.env`, and Helm/K8s secret exclusions before `git init`.