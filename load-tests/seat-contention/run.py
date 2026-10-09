#!/usr/bin/env python3
"""Seat-contention load test against the running compose stack.

    python3 load-tests/seat-contention/run.py                  # Redis contention filter on
    python3 load-tests/seat-contention/run.py --without-redis  # Redis stopped: Postgres alone must hold

Needs the `inventory` (or `services`) compose profile up, Docker, and Python 3 — k6 runs from its image.
Each run uses a train-date nobody has touched, then checks the invariants in Postgres itself:
k6's counts can only show what clients saw, the database shows what actually happened.
"""
import argparse
import os
import random
import subprocess
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
import keycloak  # noqa: E402

K6_IMAGE = "grafana/k6:1.3.0"
POSTGRES = "train-booking-postgres-1"
REDIS = "train-booking-redis-1"
TRAIN = "ER201"           # 12 BUSINESS (B1–B12) + 48 ECONOMY seats — inventory-service application.yml
BUSINESS_SEATS = [f"B{i}" for i in range(1, 13)]
ECONOMY_SEATS = 48
HORIZON_DAYS = 89         # inventory.booking-horizon is 90d


def sql(query):
    out = subprocess.run(["docker", "exec", POSTGRES, "psql", "-U", "postgres", "-d", "inventory", "-tAc", query],
                         capture_output=True, text=True, check=True).stdout.strip()
    return out


def fresh_travel_date():
    used = set(sql(f"SELECT DISTINCT travel_date FROM seat_inventory WHERE train_number = '{TRAIN}'").split())
    candidates = [d for d in (date.today() + timedelta(days=n) for n in range(2, HORIZON_DAYS)) if d.isoformat() not in used]
    if not candidates:
        sys.exit(f"every {TRAIN} date within the horizon has been used; reset the inventory database")
    return random.choice(candidates).isoformat()


def check_invariants(travel_date):
    where = f"train_number = '{TRAIN}' AND travel_date = '{travel_date}'"
    expected = len(BUSINESS_SEATS) + ECONOMY_SEATS
    checks = {
        "every seat is held": (sql(f"SELECT count(*) FROM seat_inventory WHERE {where} AND status = 'HELD'"), expected),
        "one active hold per seat": (sql(f"SELECT count(*) FROM seat_hold WHERE {where} AND status = 'ACTIVE'"), expected),
        "no seat with two active holds": (sql(f"""SELECT count(*) FROM (SELECT seat_id FROM seat_hold WHERE {where}
                AND status = 'ACTIVE' GROUP BY seat_id HAVING count(*) > 1) d"""), 0),
        "every held seat points at its own hold": (sql(f"""SELECT count(*) FROM seat_inventory s WHERE {where.replace('train_number', 's.train_number').replace('travel_date', 's.travel_date')}
                AND s.status = 'HELD' AND NOT EXISTS (SELECT 1 FROM seat_hold h WHERE h.id = s.hold_id
                AND h.seat_id = s.id AND h.status = 'ACTIVE')"""), 0),
        "exactly one seat.held event per hold": (sql(f"""SELECT count(*) FROM outbox WHERE topic = 'seat.held.v1'
                AND payload->>'trainNumber' = '{TRAIN}' AND payload->>'travelDate' = '{travel_date}'"""), expected),
    }
    ok = True
    for label, (actual, want) in checks.items():
        passed = int(actual) == want
        ok &= passed
        print(f"  {'PASS' if passed else 'FAIL'} {label}  (got {actual}, want {want})")
    return ok


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--without-redis", action="store_true", help="stop Redis for the run (restarted afterwards)")
    parser.add_argument("--vus", type=int, default=4, help="VUs attacking the hot seats together")
    parser.add_argument("--batch", type=int, default=25, help="parallel requests per VU per seat")
    parser.add_argument("--any-seat-requests", type=int, default=150, help="concurrent 'any ECONOMY seat' holds")
    parser.add_argument("--base-url", default="http://host.docker.internal:8082", help="inventory-service, seen from Docker")
    args = parser.parse_args()

    tokens = [keycloak.login(user)["access_token"] for user in ("aisha", "omar")]
    travel_date = fresh_travel_date()
    print(f"== {TRAIN} {travel_date}: {args.vus * args.batch} concurrent holds per BUSINESS seat, "
          f"{args.any_seat_requests} for {ECONOMY_SEATS} ECONOMY seats, Redis {'DOWN' if args.without_redis else 'up'}")

    if args.without_redis:
        subprocess.run(["docker", "stop", REDIS], check=True, capture_output=True)
    try:
        here = os.path.abspath(os.path.dirname(__file__))
        env = {"BASE_URL": args.base_url, "TOKENS": ",".join(tokens), "TRAIN": TRAIN, "TRAVEL_DATE": travel_date,
               "HOT_SEATS": ",".join(BUSINESS_SEATS), "VUS": args.vus, "BATCH": args.batch,
               "ANY_SEAT_REQUESTS": args.any_seat_requests, "ECONOMY_SEATS": ECONOMY_SEATS}
        cmd = ["docker", "run", "--rm", "-v", f"{here}:/scripts:ro", K6_IMAGE, "run", "--quiet"]
        for k, v in env.items():
            cmd += ["-e", f"{k}={v}"]
        k6 = subprocess.run(cmd + ["/scripts/seat-contention.js"])
    finally:
        if args.without_redis:
            subprocess.run(["docker", "start", REDIS], check=True, capture_output=True)

    print("== Invariants in Postgres")
    ok = check_invariants(travel_date)
    print(f"  {'PASS' if k6.returncode == 0 else 'FAIL'} k6 thresholds (winners, no 5xx, p95 < 2 s)")
    sys.exit(0 if ok and k6.returncode == 0 else 1)


if __name__ == "__main__":
    main()
