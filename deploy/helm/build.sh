#!/usr/bin/env bash
# Resolves chart dependencies (leaf charts first — each vendors its own copy of `common`, so the
# umbrella must be rebuilt after them), then lints, renders and schema-validates the umbrella chart
# with default and kind values. Needs only Docker; helm/kubeconform run from their images.
set -euo pipefail
cd "$(dirname "$0")"
HELM_IMAGE=alpine/helm:4.3.0
KUBECONFORM_IMAGE=ghcr.io/yannh/kubeconform:v0.7.0
K8S_VERSION=1.34.0
OUT=${OUT:-$(mktemp -d)}

helm() { docker run --rm -v "$PWD":/charts -w /charts "$HELM_IMAGE" "$@"; }

for chart in services/* streams/*; do helm dependency update "$chart" >/dev/null; done
helm dependency update train-booking >/dev/null
helm lint --strict train-booking --with-subcharts

for values in default kind; do
  args=(template tb train-booking --namespace train-booking)
  [[ $values == kind ]] && args+=(-f values-kind.yaml)
  helm "${args[@]}" > "$OUT/$values.yaml"
  echo "== $values: $(grep -c '^kind:' "$OUT/$values.yaml") resources"
  docker run --rm -v "$OUT":/out "$KUBECONFORM_IMAGE" -strict -summary \
    -kubernetes-version "$K8S_VERSION" \
    -schema-location default \
    -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
    "/out/$values.yaml"
done
echo "rendered manifests: $OUT"
