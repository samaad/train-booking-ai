{{- define "common.serviceaccount" -}}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "common.name" . }}
  labels:
    {{- include "common.labels" . | nindent 4 }}
# No service needs the Kubernetes API.
automountServiceAccountToken: false
{{- end }}

{{- define "common.configmap" -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "common.name" . }}
  labels:
    {{- include "common.labels" . | nindent 4 }}
data:
  {{- include "common.env" . | trim | nindent 2 }}
{{- end }}

{{/*
Secrets come from an external store (External Secrets Operator), never from values files.
Remote key: <global.externalSecrets.keyPrefix>/<service>, one property per variable.
*/}}
{{- define "common.externalsecret" -}}
{{- $g := .Values.global | default dict -}}
{{- if and (include "common.hasSecrets" .) (dig "externalSecrets" "enabled" true $g) -}}
{{- $name := include "common.name" . -}}
{{- $prefix := dig "externalSecrets" "keyPrefix" "train-booking" $g -}}
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: {{ $name }}
  labels:
    {{- include "common.labels" . | nindent 4 }}
spec:
  refreshInterval: {{ dig "externalSecrets" "refreshInterval" "1h" $g }}
  secretStoreRef:
    kind: {{ dig "externalSecrets" "storeKind" "ClusterSecretStore" $g }}
    name: {{ dig "externalSecrets" "storeName" "train-booking" $g }}
  target:
    name: {{ $name }}
    creationPolicy: Owner
  data:
    {{- range $envName := keys .Values.secretEnv | sortAlpha }}
    - secretKey: {{ $envName }}
      remoteRef:
        key: {{ printf "%s/%s" $prefix $name }}
        property: {{ get $.Values.secretEnv $envName }}
    {{- end }}
{{- end }}
{{- end }}

{{- define "common.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "common.name" . }}
  labels:
    {{- include "common.labels" . | nindent 4 }}
spec:
  type: ClusterIP
  selector:
    {{- include "common.selectorLabels" . | nindent 4 }}
  ports:
    - name: http
      port: {{ .Values.port }}
      targetPort: http
      protocol: TCP
{{- end }}

{{- define "common.deployment" -}}
{{- $name := include "common.name" . -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ $name }}
  labels:
    {{- include "common.labels" . | nindent 4 }}
spec:
  {{- if not .Values.hpa.enabled }}
  replicas: {{ .Values.replicas }}
  {{- end }}
  revisionHistoryLimit: 3
  strategy:
    {{- toYaml .Values.strategy | nindent 4 }}
  selector:
    matchLabels:
      {{- include "common.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "common.labels" . | nindent 8 }}
      annotations:
        # Env comes from the ConfigMap via envFrom, which does not restart pods on its own.
        checksum/config: {{ include "common.env" . | sha256sum }}
    spec:
      serviceAccountName: {{ $name }}
      automountServiceAccountToken: false
      # preStop sleep + Spring's graceful shutdown (30 s per phase) + margin.
      terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001
        seccompProfile:
          type: RuntimeDefault
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              {{- include "common.selectorLabels" . | nindent 14 }}
      {{- with .Values.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      containers:
        - name: app
          image: {{ include "common.image" . }}
          imagePullPolicy: {{ .Values.image.pullPolicy | default (dig "image" "pullPolicy" "IfNotPresent" (.Values.global | default dict)) }}
          ports:
            - name: http
              containerPort: {{ .Values.port }}
              protocol: TCP
          envFrom:
            - configMapRef:
                name: {{ $name }}
            {{- if include "common.hasSecrets" . }}
            - secretRef:
                name: {{ $name }}
            {{- end }}
          resources:
            {{- toYaml .Values.resources | nindent 12 }}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          # Startup covers JVM boot + Flyway; liveness only takes over once it passes.
          startupProbe:
            httpGet: { path: /actuator/health/liveness, port: http }
            periodSeconds: 5
            failureThreshold: {{ .Values.probes.startupFailureThreshold }}
          livenessProbe:
            httpGet: { path: /actuator/health/liveness, port: http }
            periodSeconds: 10
            timeoutSeconds: 3
            failureThreshold: 3
          readinessProbe:
            httpGet: { path: /actuator/health/readiness, port: http }
            periodSeconds: 5
            timeoutSeconds: 3
            failureThreshold: 3
          lifecycle:
            # Give endpoints/kube-proxy time to drop the pod before Spring stops accepting requests.
            preStop:
              sleep:
                seconds: 10
          volumeMounts:
            # Read-only root: the JVM (hsperfdata) and Netty (native libs) need a writable /tmp.
            - name: tmp
              mountPath: /tmp
            {{- range .Values.writableDirs }}
            - name: {{ .name }}
              mountPath: {{ .mountPath }}
            {{- end }}
      volumes:
        - name: tmp
          emptyDir:
            sizeLimit: 256Mi
        {{- range .Values.writableDirs }}
        - name: {{ .name }}
          emptyDir:
            sizeLimit: {{ .sizeLimit | default "1Gi" }}
        {{- end }}
{{- end }}

{{- define "common.hpa" -}}
{{- if .Values.hpa.enabled -}}
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ include "common.name" . }}
  labels:
    {{- include "common.labels" . | nindent 4 }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{ include "common.name" . }}
  minReplicas: {{ .Values.hpa.minReplicas }}
  maxReplicas: {{ .Values.hpa.maxReplicas }}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {{ .Values.hpa.cpuUtilization }}
    {{- with .Values.hpa.podsMetric }}
    # Served by prometheus-adapter — see deploy/helm/prometheus-adapter-values.yaml.
    - type: Pods
      pods:
        metric:
          name: {{ .name }}
        target:
          type: AverageValue
          averageValue: {{ .averageValue | quote }}
    {{- end }}
  behavior:
    scaleDown:
      # JVMs are slow to warm up; don't flap.
      stabilizationWindowSeconds: 300
{{- end }}
{{- end }}

{{- define "common.pdb" -}}
{{- if and .Values.pdb.enabled (gt (int (include "common.minPods" .)) 1) -}}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "common.name" . }}
  labels:
    {{- include "common.labels" . | nindent 4 }}
spec:
  minAvailable: 1
  selector:
    matchLabels:
      {{- include "common.selectorLabels" . | nindent 6 }}
{{- end }}
{{- end }}

{{- define "common.servicemonitor" -}}
{{- if .Values.serviceMonitor.enabled -}}
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "common.name" . }}
  labels:
    {{- include "common.labels" . | nindent 4 }}
    {{- with dig "serviceMonitor" "labels" (dict) (.Values.global | default dict) }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  selector:
    matchLabels:
      {{- include "common.selectorLabels" . | nindent 6 }}
  endpoints:
    - port: http
      path: /actuator/prometheus
      interval: {{ .Values.serviceMonitor.interval }}
{{- end }}
{{- end }}

{{/* Everything a service chart renders: `{{ include "common.all" . }}`. */}}
{{- define "common.all" -}}
{{- $docs := list -}}
{{- range $t := list "common.serviceaccount" "common.configmap" "common.externalsecret" "common.service" "common.deployment" "common.hpa" "common.pdb" "common.servicemonitor" }}
{{- $doc := include $t $ | trim -}}
{{- if $doc }}{{ $docs = append $docs $doc }}{{ end -}}
{{- end }}
{{- join "\n---\n" $docs }}
{{- end }}
