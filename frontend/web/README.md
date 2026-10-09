# train-web

React 19 + Vite + TypeScript client for the train booking system. Everything goes through Kong
(`http://localhost:8000/api/...`); sign-in is Authorization Code + PKCE against Keycloak's public
`train-web` client via `oidc-client-ts`.

```bash
npm install
npm run dev        # http://localhost:5173 — the only origin Keycloak, Kong CORS and the seat-map WS accept
npm test           # vitest
npm run lint       # oxlint
npm run build      # tsc -b && vite build → dist/
```

Backend: `docker compose -f infra/docker-compose.yml --profile services --profile gateway --profile assistant up -d --wait`
(Kong is the `gateway` profile). Dev users `aisha` / `omar` / `agent` / `admin`, password `Passw0rd-dev`.
Endpoints are configured in `.env` (`VITE_API_BASE`, `VITE_OIDC_AUTHORITY`, `VITE_OIDC_CLIENT_ID`).

## Screens

| Route | What it does |
|---|---|
| `/` | Station search (catalog, public) with per-class free-seat counts (inventory, public) |
| `/trains/:train/:date` | Seat map: inventory snapshot + live updates from `seat-availability-streams` over WebSocket; hold a seat |
| `/checkout/:holdId` | Passenger details with hold countdown → `POST /bookings` |
| `/trips/:id/pay` | Fare as priced by booking-service → `POST /payments`, then follows the saga until ticketed |
| `/trips`, `/trips/:id` | Your bookings (`GET /bookings`), ticket with QR and PDF, cancel |
| `/assistant/:conversationId?` | Chat over SSE with tool cards and confirmation cards |
| `/profile` | Preferences (optimistic `version`) and saved travellers |

## Rules this client keeps

- **Idempotency keys belong to an intent**, not a request (`lib/idempotency.ts`). Holding a seat, booking a
  hold and paying a booking each mint one key, kept in `sessionStorage`; a retry after a timeout or a 503
  replays the same key, so it can never double-book or double-charge. Mutations are never auto-retried.
- **Prices are never computed here.** The fare shown before paying is the one booking-service stored.
- **The assistant proposes; the user disposes.** `confirmation` events render a card; only a click sends the
  request, with the server-derived idempotency key. `lib/confirmation.ts` only lets a card call the two known
  booking endpoints, whatever the event says.
- **Times are shown in Asia/Dubai.** The API sends UTC; `lib/format.ts` converts. Travel dates are calendar
  dates and are never shifted.
- `/chat` is a `POST` with a bearer token, which `EventSource` cannot do, so `api/sse.ts` parses the stream
  from `fetch`.
- Tokens live in `sessionStorage` and renew with the refresh token. Roles and loyalty tier are read from the
  access token for display only — every service validates the token itself.

## Local payments

payment-service talks to a stub PSP that takes a token, never card data. The payment step offers three test
tokens: `tok_visa_…` approves, `tok_decline…` declines (the saga then cancels and releases the seat), and
`tok_unavailable…` simulates a PSP outage (503 — "Try again" replays the same key).
