# Helm charts

**Authoring only — there is no cluster here.** The charts are linted, rendered and schema-validated
(`./build.sh`, Docker only); they have never been installed. Treat them as a reviewed starting point.

```
deploy/helm/
├── common/                      library chart: every template (Deployment, Service, HPA, PDB,
│                                ServiceMonitor, ConfigMap, ExternalSecret, ServiceAccount)
├── services/<service>/          one chart per service — Chart.yaml + values.yaml only
├── streams/seat-availability-streams/
├── train-booking/               umbrella: global settings, Ollama Service, enables each chart
├── values-kind.yaml             laptop overrides (1 replica, no HPA/ExternalSecrets/ServiceMonitors)
├── prometheus-adapter-values.yaml   custom metrics the HPAs scale on
└── build.sh                     dependency update → lint → template → kubeconform
```

`./build.sh` must rebuild leaf charts before the umbrella: each service chart vendors its own copy
of `common`, so editing `common` and updating only the umbrella renders the **old** templates.

## What every service gets

| | |
|---|---|
| Deployment | 2 replicas (HPA-managed when enabled), rolling update with `maxUnavailable: 0`, spread across nodes |
| Probes | startup + liveness on `/actuator/health/liveness`, readiness on `/actuator/health/readiness` |
| Shutdown | `preStop` sleep 10 s, then Spring's graceful shutdown; grace period 45 s |
| Pod security | UID 10001 (pinned in `service.Dockerfile`), non-root, read-only root FS (`/tmp` is an emptyDir), all capabilities dropped, `RuntimeDefault` seccomp, no SA token |
| Config | ConfigMap via `envFrom` (checksum annotation rolls pods on change); shared values picked by `uses` |
| Secrets | `ExternalSecret` (External Secrets Operator) → Secret via `envFrom`; never in values files |
| Scaling | HPA on CPU + a per-pod Prometheus metric (see below); PDB `minAvailable: 1` when ≥ 2 pods |
| Metrics | `ServiceMonitor` on `/actuator/prometheus` |

Resource names are the bare service name, so in-cluster URLs match compose
(`http://inventory-service:8082`). Install **one release per namespace**.

### Exceptions

- **ai-assistant-service**: 1–2 replicas, 1–1.5 GiB, scales on `ai_scheduler_queued` rather than
  CPU (it mostly waits on the model). Extra replicas only help if Ollama has headroom; in a real
  cluster, set a GPU `nodeSelector`.
- **seat-availability-streams**: exactly one replica with `Recreate`. `/occupancy` and the live
  seat map read the *local* state store; a second instance would own half the partitions. Scale
  out only after adding `application.server` + metadata routing.
- **notification-service** scales on consumer lag; replicas beyond the partition count sit idle.

## Not in these charts

Postgres, MongoDB, Redis, Kafka, Keycloak, Kong and the observability stack are prerequisites wired in
through `global` (e.g. CloudNativePG, Strimzi, the Keycloak operator, Kong's own chart with
`infra/kong/kong.yml`, kube-prometheus-stack). Ollama stays outside: the assistant always calls
`http://ollama:11434`, and the umbrella's `ollama` Service decides where that goes — `ExternalName`
to the host for kind/minikube, or `mode: none` if you run a GPU-backed Ollama Service yourself.

Secrets expected in the store, under `train-booking/<service>`:

| Service | Properties |
|---|---|
| inventory, payment, user-profile, ai-assistant | `db-password` |
| booking | `db-password`, `client-secret` (Keycloak `train-services` client) |
| ticketing | `db-password`, `qr-secret`, `object-store-access-key`, `object-store-secret-key` |

## Custom metrics

The HPAs' `Pods` metrics come from prometheus-adapter:

```
helm install prometheus-adapter prometheus-community/prometheus-adapter -n monitoring \
  -f deploy/helm/prometheus-adapter-values.yaml
```

| HPA metric | Source |
|---|---|
| `http_server_requests_per_second` | `rate(http_server_requests_seconds_count[2m])`, actuator excluded |
| `ai_scheduler_queued` | the assistant's bounded scheduler queue |
| `kafka_consumer_records_lag_max` | `kafka_consumer_fetch_manager_records_lag_max` |

## kind (untested sketch)

```
./mvnw -q clean package -DskipTests
docker compose -f infra/docker-compose.yml --profile services build
kind create cluster --name train-booking
for s in catalog-service inventory-service booking-service payment-service ticketing-service \
         notification-service user-profile-service ai-assistant-service seat-availability-streams; do
  kind load docker-image train-booking/$s:dev --name train-booking
done
# values-kind.yaml expects the compose infra under its compose names (postgres, kafka:29092, …) in
# the release namespace, and one Secret per service named after it (ExternalSecrets are off), e.g.
kubectl create secret generic booking-service \
  --from-literal=DB_PASSWORD=booking --from-literal=TRAIN_SERVICES_SECRET=<dev secret>
helm install tb deploy/helm/train-booking -f deploy/helm/values-kind.yaml
```

Docker Desktop here has ~8 GB: the infra plus nine JVMs will not fit, so enable a slice
(`--set booking-service.enabled=false`, …).
