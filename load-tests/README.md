# Load tests

Run against the compose stack; nothing to install beyond Docker and Python 3 (k6 runs from its image,
logins use only the standard library).

## Seat contention

The claim under test (03-consistency): **Postgres alone decides who holds a seat** — Redis only sheds load.

```
docker compose -f infra/docker-compose.yml --profile inventory up -d --wait
python3 load-tests/seat-contention/run.py                  # contention filter on
python3 load-tests/seat-contention/run.py --without-redis  # Redis stopped for the run, restarted after
```

Each run picks a train-date nobody has used (ER201: 12 BUSINESS + 48 ECONOMY seats) and fires, against
inventory-service directly (Kong's rate limits would only measure Kong):

| Scenario | Load | Must hold |
|---|---|---|
| hot seats | 4 VUs × 25 parallel holds on **each** BUSINESS seat — 100 per seat | exactly 12 × 201, the rest 409 `SEAT_UNAVAILABLE` |
| sell-out | 150 parallel "any ECONOMY seat" holds | exactly 48 × 201, the rest 409 `SOLD_OUT` |
| both | — | no 5xx, p95 < 2 s |

Then it checks Postgres itself — k6 only sees what clients saw:

- every seat HELD, exactly one ACTIVE hold per seat, no seat with two, each held seat pointing at its own hold;
- exactly one `seat.held.v1` outbox event per hold.

Results on this machine (Oct 2026, 1 300 requests per run):

| | winners | 5xx | median | p95 |
|---|---|---|---|---|
| Redis up | 60 / 60 | 0 | 65 ms | 0.9 s |
| Redis down | 60 / 60 | 0 | 640 ms | 0.7 s |

With Redis down every request first waits out the filter's 300 ms Redis budget, then goes to Postgres —
slower, still exactly correct. That is the design: the filter is an optimisation, never the lock.
