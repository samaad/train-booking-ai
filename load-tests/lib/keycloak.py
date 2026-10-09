"""Signs dev users in the way the SPA does — Authorization Code + PKCE against the local realm.

The realm deliberately has no password-grant client, so tooling logs in like a browser would. Standard
library only, so load and replay tools run without a virtualenv.
"""
import base64
import hashlib
import html
import http.cookiejar
import json
import os
import re
import secrets
import urllib.parse
import urllib.request

KEYCLOAK = "http://localhost:8180/realms/train-booking/protocol/openid-connect"
REDIRECT = "http://localhost:5173/callback"
DEV_PASSWORD = os.environ.get("DEV_USER_PASSWORD", "Passw0rd-dev")  # dev realm only (infra/keycloak/build-realm.sh)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class _LocalhostIsSecure(http.cookiejar.DefaultCookiePolicy):
    """Keycloak marks its session cookies Secure; treat plain-http localhost as secure, as browsers do."""

    def return_ok_secure(self, cookie, request):
        return True


def login(username, scope="openid booking:read booking:write", password=DEV_PASSWORD):
    """Returns the token response (access_token, …); raises RuntimeError with Keycloak's reason otherwise."""
    jar = http.cookiejar.CookieJar(policy=_LocalhostIsSecure())
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar), _NoRedirect)
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    query = urllib.parse.urlencode({"client_id": "train-web", "response_type": "code", "redirect_uri": REDIRECT,
                                    "scope": scope, "state": secrets.token_urlsafe(8),
                                    "code_challenge": challenge, "code_challenge_method": "S256"})
    page = opener.open(f"{KEYCLOAK}/auth?{query}").read().decode()
    form = re.search(r'action="([^"]+)"', page)
    if not form:
        raise RuntimeError(f"no Keycloak login form for {username}")
    try:
        opener.open(html.unescape(form.group(1)),
                    urllib.parse.urlencode({"username": username, "password": password}).encode())
        raise RuntimeError(f"login for {username} did not redirect (wrong password?)")
    except urllib.error.HTTPError as redirect:
        location = redirect.headers.get("Location", "")
    code = urllib.parse.parse_qs(urllib.parse.urlparse(location).query).get("code", [None])[0]
    if not code:
        raise RuntimeError(f"no authorization code for {username}: {location[:120]}")
    token = urllib.parse.urlencode({"grant_type": "authorization_code", "client_id": "train-web", "code": code,
                                    "redirect_uri": REDIRECT, "code_verifier": verifier}).encode()
    return json.loads(urllib.request.urlopen(f"{KEYCLOAK}/token", token).read())
