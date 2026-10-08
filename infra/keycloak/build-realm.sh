#!/usr/bin/env bash
# Builds the `train-booking` realm (05-security) on a running Keycloak and exports it to
# infra/keycloak/realm-export.json, which compose imports on start (--import-realm).
#
# The realm is defined here, as readable kcadm calls, rather than as hand-written JSON: declaring custom
# client scopes in an import file suppresses Keycloak's built-in ones (roles, basic, ...) and tokens then
# silently lose realm_access and sub. Re-run this script, not a JSON edit, to change the realm.
#
#   docker compose -f infra/docker-compose.yml --profile auth up -d --wait
#   infra/keycloak/build-realm.sh
#
# DEV ONLY: users get a known password and train-services a placeholder secret. Never import into prod.
set -euo pipefail

KC=${KC_CONTAINER:-train-booking-keycloak-1}
REALM=train-booking
DEV_PASSWORD=${DEV_USER_PASSWORD:-Passw0rd-dev}
HERE=$(cd "$(dirname "$0")" && pwd)

k() { docker exec "$KC" /opt/keycloak/bin/kcadm.sh "$@"; }
id_of() { k get "$1" -r "$REALM" -q "$2" --fields id --format csv --noquotes | head -1; }
# The client-scopes endpoint ignores -q filters, so match the exact name ourselves.
scope_id() {
  local id
  id=$(k get client-scopes -r "$REALM" --fields id,name --format csv --noquotes | awk -F, -v n="$1" '$2 == n {print $1}')
  [ -n "$id" ] || { echo "no client scope $1" >&2; exit 1; }
  echo "$id"
}

k config credentials --server http://localhost:8080 --realm master --user "${KEYCLOAK_ADMIN:-admin}" \
  --password "${KEYCLOAK_ADMIN_PASSWORD:-admin}" >/dev/null
k delete "realms/$REALM" >/dev/null 2>&1 || true

# Short access tokens matter: JWT validation has no revocation. Refresh lives 30 minutes.
k create realms -s realm="$REALM" -s enabled=true -s sslRequired=external \
  -s accessTokenLifespan=300 -s ssoSessionIdleTimeout=1800 -s ssoSessionMaxLifespan=36000 \
  -s registrationAllowed=false -s loginWithEmailAllowed=true -s bruteForceProtected=true

# Keycloak 26's declarative user profile silently drops undeclared attributes, so declare loyalty_tier —
# admin-editable only: a traveller must not be able to raise their own tier.
k get users/profile -r "$REALM" | python3 -c '
import json, sys
profile = json.load(sys.stdin)
profile["attributes"].append({"name": "loyalty_tier", "displayName": "Loyalty tier",
    "permissions": {"view": ["admin", "user"], "edit": ["admin"]}, "multivalued": False})
json.dump(profile, sys.stdout)' > "$HERE/.profile.json"
docker cp "$HERE/.profile.json" "$KC:/tmp/profile.json" >/dev/null && rm -f "$HERE/.profile.json"
k update users/profile -r "$REALM" -f /tmp/profile.json

for role in PASSENGER AGENT SUPPORT ADMIN SERVICE; do
  k create roles -r "$REALM" -s name="$role"
done
# Everyone who signs up is a passenger.
k add-roles -r "$REALM" --rname "default-roles-$REALM" --rolename PASSENGER

# Fine-grained API scopes: optional, so a client asks for what it needs and a token can lack one (→ 403).
for scope in booking:read booking:write payment:write profile:read profile:write ai:chat; do
  k create client-scopes -r "$REALM" -s name="$scope" -s protocol=openid-connect \
    -s 'attributes."include.in.token.scope"=true' -s 'attributes."display.on.consent.screen"=false'
done

# Every API validates aud=train-api.
k create client-scopes -r "$REALM" -s name=train-api-audience -s protocol=openid-connect \
  -s 'attributes."include.in.token.scope"=false'
AUD=$(scope_id train-api-audience)
k create "client-scopes/$AUD/protocol-mappers/models" -r "$REALM" -s name=train-api-audience \
  -s protocol=openid-connect -s protocolMapper=oidc-audience-mapper \
  -s 'config."included.custom.audience"=train-api' -s 'config."access.token.claim"=true' \
  -s 'config."id.token.claim"=false'

# Custom claim loyalty_tier, from a user attribute.
k create client-scopes -r "$REALM" -s name=loyalty -s protocol=openid-connect \
  -s 'attributes."include.in.token.scope"=false'
LOY=$(scope_id loyalty)
k create "client-scopes/$LOY/protocol-mappers/models" -r "$REALM" -s name=loyalty_tier \
  -s protocol=openid-connect -s protocolMapper=oidc-usermodel-attribute-mapper \
  -s 'config."user.attribute"=loyalty_tier' -s 'config."claim.name"=loyalty_tier' \
  -s 'config."jsonType.label"=String' -s 'config."access.token.claim"=true' -s 'config."id.token.claim"=true'

# Public clients: Authorization Code + PKCE (S256), no secret, no implicit, no password grant.
k create clients -r "$REALM" -s clientId=train-web -s publicClient=true -s standardFlowEnabled=true \
  -s implicitFlowEnabled=false -s directAccessGrantsEnabled=false \
  -s 'redirectUris=["http://localhost:5173/*"]' -s 'webOrigins=["http://localhost:5173"]' \
  -s 'attributes."pkce.code.challenge.method"=S256' \
  -s 'attributes."post.logout.redirect.uris"=http://localhost:5173/*'
# Mobile: app-claimed HTTPS redirect (a custom scheme can be hijacked by another installed app).
k create clients -r "$REALM" -s clientId=train-mobile -s publicClient=true -s standardFlowEnabled=true \
  -s implicitFlowEnabled=false -s directAccessGrantsEnabled=false \
  -s 'redirectUris=["https://app.trainbook.example/auth/callback"]' \
  -s 'attributes."pkce.code.challenge.method"=S256'
# Service-to-service: client credentials. The secret is replaced by a placeholder in the export.
k create clients -r "$REALM" -s clientId=train-services -s publicClient=false -s serviceAccountsEnabled=true \
  -s standardFlowEnabled=false -s directAccessGrantsEnabled=false -s secret=dev-only-train-services-secret
k add-roles -r "$REALM" --uusername service-account-train-services --rolename SERVICE
# Listed in 05-security for completeness; Kong OSS cannot validate tokens itself (see 05), services do.
k create clients -r "$REALM" -s clientId=kong-gateway -s bearerOnly=true -s publicClient=false \
  -s standardFlowEnabled=false

for client in train-web train-mobile train-services; do
  CID=$(id_of clients "clientId=$client")
  k update "clients/$CID/default-client-scopes/$AUD" -r "$REALM"
  k update "clients/$CID/default-client-scopes/$LOY" -r "$REALM"
  if [ "$client" != train-services ]; then
    for scope in booking:read booking:write payment:write profile:read profile:write ai:chat; do
      k update "clients/$CID/optional-client-scopes/$(scope_id "$scope")" -r "$REALM"
    done
  fi
done

user() { # username first last role tier
  k create users -r "$REALM" -s username="$1" -s enabled=true -s email="$1@example.com" -s emailVerified=true \
    -s firstName="$2" -s lastName="$3" -s "attributes.loyalty_tier=[\"$5\"]"
  k set-password -r "$REALM" --username "$1" --new-password "$DEV_PASSWORD"
  [ "$4" = PASSENGER ] || k add-roles -r "$REALM" --uusername "$1" --rolename "$4"
}
user aisha Aisha Khan PASSENGER GOLD
user omar Omar Haddad PASSENGER SILVER
user agent Front Desk AGENT NONE
user admin Rail Admin ADMIN NONE

echo "Realm built; exporting"
docker exec "$KC" rm -rf /tmp/realm-export
docker exec "$KC" /opt/keycloak/bin/kc.sh export --dir /tmp/realm-export --realm "$REALM" --users realm_file --http-management-port=9099 >/dev/null 2>&1
docker exec "$KC" cat "/tmp/realm-export/$REALM-realm.json" > "$HERE/realm-export.raw.json"

# Strip signing keys (each import generates its own) and template the service secret.
python3 - "$HERE/realm-export.raw.json" "$HERE/realm-export.json" <<'PY'
import json, sys
realm = json.load(open(sys.argv[1]))
realm.get("components", {}).pop("org.keycloak.keys.KeyProvider", None)
for client in realm.get("clients", []):
    if client.get("clientId") == "train-services":
        client["secret"] = "${TRAIN_SERVICES_SECRET:dev-only-train-services-secret}"
    elif "secret" in client and not client.get("publicClient"):
        client.pop("secret")  # e.g. kong-gateway: generated, unused, and never to be committed
json.dump(realm, open(sys.argv[2], "w"), indent=2, sort_keys=False)
PY
rm -f "$HERE/realm-export.raw.json"
echo "Wrote $HERE/realm-export.json"
