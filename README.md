# train-booking-ai

An AI-driven train booking system built as reactive Spring microservices. A conversational assistant,
backed by a **local** Ollama model (`gemma4`), searches trains, holds seats and prepares bookings by calling
the real services — it never invents trains, prices or bookings, and nothing irreversible happens without
the user clicking confirm.

```
React web ──► Kong (routing, CORS, rate limits) ──► services ──► Postgres / Mongo / Redis
     │                                                │
     └──── PKCE ──► Keycloak                          └──► Kafka (outbox → saga, streams)
                                     ai-assistant ──► Ollama gemma4 (on the host)
```

## Stack

Spring Boot 4.1.1 · Java 25 · Spring Cloud 2025.1.3 · Spring AI 2.0.1 · WebFlux + R2DBC ·
Postgres 18 / pgvector · MongoDB · Redis · Kafka + Kafka Streams · Keycloak 26.7 · Kong 3.16 ·
OpenTelemetry → Tempo · Prometheus / Grafana · React 19 + Vite + TypeScript

## Repository layout

| Path | Contents |
|---|---|
| `common-lib/` | Shared jars: `common-core` (ids, errors, idempotency), `common-web` (problem+json), `common-security` (JWT resource server, ownership), `common-kafka` (event contracts, outbox, idempotent consumers) |
| `services/` | Bootable services (table below) |
| `streams/` | Kafka Streams apps: `seat-availability-streams` (live seat map), `risk-signals-streams` |
| `frontend/web/` | React web client — see its [README](frontend/web/README.md) |
| `infra/` | Docker Compose (profile-gated), Keycloak realm, Kong config, Postgres init, observability, Debezium |
| `deploy/helm/` | Helm charts (authoring only — linted and rendered, not installed) |
| `load-tests/` | Seat-contention load test (k6) with database invariant checks |
| `.claude/plan/` | Architecture and delivery plan |

## Services

| Service | Port | Store | Kong route |
|---|---|---|---|
| ai-assistant-service | 8080 | Mongo + pgvector | `/api/assistant` |
| catalog-service | 8081 | Mongo | `/api/catalog` |
| inventory-service | 8082 | Postgres + Redis | `/api/inventory` |
| booking-service | 8083 | Postgres | `/api/booking` |
| payment-service | 8084 | Postgres (stub PSP) | `/api/payment` |
| ticketing-service | 8085 | Postgres + S3 (s3mock) | `/api/ticketing` |
| notification-service | 8086 | Mongo | — (events only) |
| user-profile-service | 8087 | Postgres | `/api/profile` |
| seat-availability-streams | 8089 | Kafka Streams | `/api/availability` |
| risk-signals-streams | 8091 | Kafka Streams | — |
| batch-admin-service | 8092 | — (DLT replay, ADMIN only) | — |

Other ports: Kong 8000, Keycloak 8180, web 5173, kafka-ui 8090, Prometheus 9090, Grafana 3000, Tempo 3200.

## How a booking works

1. **Search** — catalog returns schedules; inventory reports free seats per fare class.
2. **Hold** — inventory places a time-limited hold with a single conditional `UPDATE` in Postgres (Redis is
   only a contention filter). Exactly one caller wins each seat.
3. **Book** — booking-service prices the hold from catalog fare rules and writes the booking and its
   `booking.created` event in one transaction (transactional outbox).
4. **Pay** — payment-service authorises only the amount booking-service set.
5. **Saga** — payment authorised → seats committed → ticket issued, choreographed over Kafka. Any failure
   or cancellation emits `booking.cancelled`, which releases the seat, refunds and voids the ticket.

Every mutating call takes an `Idempotency-Key`; replaying it returns the original result, so retries never
double-book or double-charge.

## The AI assistant

- `POST /chat` streams Server-Sent Events: tokens, tool activity, confirmation cards, warnings, fallbacks.
- Tools call the real services with the **user's own token**; identity comes from the security context,
  never from the model.
- `confirmBooking` needs a hold the conversation actually created, and only *proposes* — the user confirms
  in the UI, which then calls booking-service itself.
- Prices quoted in the reply are checked against tool results; if the model is unavailable, the assistant
  falls back to deterministic search results.
- RAG over railway policy documents uses `mxbai-embed-large` in pgvector.

## Getting started

### Prerequisites

- JDK 25 (use the bundled `./mvnw` — there is no system Maven requirement)
- Docker Desktop with **at least 8 GB** of memory
- Node.js 22+ for the web client
- [Ollama](https://ollama.com) on the host with `OLLAMA_HOST=0.0.0.0`, plus the models:
  ```bash
  ollama pull gemma4
  ollama pull mxbai-embed-large
  ```

### Run it

```bash
# 1. Build the service jars (images copy the host-built jars; `clean` avoids stale shared libs)
./mvnw -q clean package -DskipTests

# 2. Start infrastructure, all services, Kong and the assistant
docker compose -f infra/docker-compose.yml \
  --profile services --profile gateway --profile assistant up -d --build --wait

# 3. Start the web client
cd frontend/web && npm install && npm run dev
```

Open <http://localhost:5173> and sign in.

> If host port 5432 is taken, set `POSTGRES_PORT=5433` in `infra/.env` (gitignored).

### Dev logins

Realm `train-booking`, web client `train-web` (Authorization Code + PKCE). Every user's password is
**`Passw0rd-dev`** (capital P, zero instead of "o").

| Username | Role | Loyalty tier |
|---|---|---|
| `aisha` | Passenger | GOLD |
| `omar` | Passenger | SILVER |
| `agent` | AGENT (staff) | — |
| `admin` | ADMIN | — |

The Keycloak **admin console** (<http://localhost:8180/admin>, `master` realm) is a separate login:
`admin` / `admin`.

### Test payments

payment-service uses a stub PSP that takes a token, never card data. The web client offers test tokens:
`tok_visa_…` approves, `tok_decline…` declines (the booking is cancelled and the seat released), and
`tok_unavailable…` simulates a PSP outage (retrying replays the same key safely).

## Compose profiles

Docker Desktop is capped at ~8 GB, so the stack is split into profiles rather than started all at once.

| Profile | Starts |
|---|---|
| `services` | Infra plus all seven core services and `seat-availability-streams` |
| `gateway` | Kong |
| `assistant` | ai-assistant-service (Ollama stays on the host) |
| `observability` | Prometheus, Grafana, Tempo |
| `kafka` | Kafka + kafka-ui |
| `risk`, `admin`, `cdc` | risk-signals-streams, batch-admin-service, Debezium Kafka Connect |
| `<service name>` | One service and just the infrastructure it needs |

`full` starts everything (~9 GB) and does not fit in an 8 GB Docker allocation.

## Development

```bash
./mvnw test                                   # all modules (Testcontainers; stop the compose stack first)
./mvnw -pl services/booking-service -am test  # one service
cd frontend/web && npm test && npm run lint   # web client
load-tests/seat-contention/run.py             # seat-contention invariant test (stack must be up)
deploy/helm/build.sh                          # lint and render the Helm charts
```

Don't run the full Maven build while the compose `services` stack is up — Testcontainers starves for memory.

## Further reading

- [Architecture and delivery plan](.claude/plan/README.md) — services, AI layer, consistency, Kafka,
  security, resilience, observability, deployment, roadmap
- [CLAUDE.md](CLAUDE.md) — verified version constraints and conventions every change must keep
- [Web client](frontend/web/README.md) · [Helm charts](deploy/helm/README.md) · [Load tests](load-tests/README.md)
