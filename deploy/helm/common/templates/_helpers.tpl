{{/*
Resource names are the bare service name, so in-cluster URLs (http://inventory-service:8082) match
compose. One release per namespace.
*/}}
{{- define "common.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "common.selectorLabels" -}}
app.kubernetes.io/name: {{ include "common.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "common.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "common.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/component: {{ .Values.component | default "service" }}
app.kubernetes.io/part-of: train-booking
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "common.image" -}}
{{- $g := .Values.global | default dict -}}
{{- $repo := .Values.image.repository -}}
{{- with dig "image" "registry" "" $g }}{{ $repo = printf "%s/%s" . $repo }}{{ end -}}
{{- printf "%s:%s" $repo (.Values.image.tag | default (dig "image" "tag" "" $g) | default .Chart.AppVersion) -}}
{{- end }}

{{/* Number of pods at steady state, used to decide whether a PDB is meaningful. */}}
{{- define "common.minPods" -}}
{{- if .Values.hpa.enabled }}{{ .Values.hpa.minReplicas }}{{ else }}{{ .Values.replicas }}{{ end -}}
{{- end }}

{{- define "common.hasSecrets" -}}
{{- if .Values.secretEnv }}true{{ end -}}
{{- end }}

{{/*
Non-secret environment: shared settings selected by `uses`, then the service's own `env` (which wins).
Secrets never come through here — see secretEnv / the ExternalSecret.
*/}}
{{- define "common.env" -}}
{{- $g := .Values.global | default dict -}}
{{- $uses := .Values.uses | default list -}}
{{- $env := dict "PORT" (toString .Values.port) "JAVA_TOOL_OPTIONS" .Values.javaOpts "LOGGING_STRUCTURED_FORMAT_CONSOLE" "ecs" -}}
{{- $_ := set $env "TRACING_SAMPLING_PROBABILITY" (toString (dig "tracing" "samplingProbability" "0.1" $g)) -}}
{{- with dig "tracing" "otlpEndpoint" "" $g }}{{ $_ := set $env "MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT" . }}{{ end -}}
{{- if has "oidc" $uses }}
{{- $_ := set $env "OIDC_ISSUER" (dig "oidc" "issuer" "" $g) -}}
{{- $_ := set $env "OIDC_JWKS" (dig "oidc" "jwks" "" $g) -}}
{{- end }}
{{- if has "serviceTokens" $uses }}{{ $_ := set $env "OIDC_TOKEN_URI" (dig "oidc" "tokenUri" "" $g) }}{{ end -}}
{{- if has "postgres" $uses }}
{{- $_ := set $env "DB_HOST" (dig "postgres" "host" "" $g) -}}
{{- $_ := set $env "DB_PORT" (toString (dig "postgres" "port" 5432 $g)) -}}
{{- end }}
{{- if has "mongo" $uses }}
{{- $_ := set $env "MONGO_HOST" (dig "mongo" "host" "" $g) -}}
{{- $_ := set $env "MONGO_PORT" (toString (dig "mongo" "port" 27017 $g)) -}}
{{- end }}
{{- if has "redis" $uses }}
{{- $_ := set $env "REDIS_HOST" (dig "redis" "host" "" $g) -}}
{{- $_ := set $env "REDIS_PORT" (toString (dig "redis" "port" 6379 $g)) -}}
{{- end }}
{{- if has "kafka" $uses }}{{ $_ := set $env "KAFKA_BOOTSTRAP" (dig "kafka" "bootstrap" "" $g) }}{{ end -}}
{{- range $k, $v := .Values.env }}{{ $_ := set $env $k (toString $v) }}{{ end -}}
{{- range $k := keys $env | sortAlpha }}
{{ $k }}: {{ get $env $k | quote }}
{{- end }}
{{- end }}
