// Seat-contention load test (03-consistency, Phase 7). Driven by run.py, which supplies tokens, a fresh
// train-date and afterwards checks the database invariants; this script only generates the contention.
//
//   hot_seats  every BUSINESS seat is attacked by VUS × BATCH concurrent holds for that exact seat
//              → exactly one 201 per seat, everything else 409.
//   sell_out   ANY_SEAT_REQUESTS concurrent "any ECONOMY seat" holds against a smaller capacity
//              → exactly as many 201s as there are seats, the rest 409 SOLD_OUT.
// No request may end in a 5xx.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://host.docker.internal:8082';
const TOKENS = (__ENV.TOKENS || '').split(',').filter(Boolean);
const TRAIN = __ENV.TRAIN;
const DATE = __ENV.TRAVEL_DATE;
const HOT_SEATS = (__ENV.HOT_SEATS || '').split(',').filter(Boolean);
const VUS = Number(__ENV.VUS || 4);
const BATCH = Number(__ENV.BATCH || 25);
const ANY_SEAT_REQUESTS = Number(__ENV.ANY_SEAT_REQUESTS || 150);
const ECONOMY_SEATS = Number(__ENV.ECONOMY_SEATS || 48);

const won = new Counter('holds_won');
const lost = new Counter('holds_lost');
const serverErrors = new Counter('server_errors');
const unexpected = new Counter('unexpected_status');

// 409 is the expected answer for losers — it must not count as a failed request.
http.setResponseCallback(http.expectedStatuses(201, 409));

export const options = {
  batchPerHost: BATCH,
  scenarios: {
    // Every VU walks the seats in the same order, so ~VUS×BATCH requests hit each seat at once.
    hot_seats: { executor: 'per-vu-iterations', vus: VUS, iterations: HOT_SEATS.length, exec: 'hotSeats', maxDuration: '2m' },
    sell_out: {
      executor: 'per-vu-iterations', vus: Math.ceil(ANY_SEAT_REQUESTS / BATCH), iterations: 1, exec: 'sellOut',
      startTime: '0s', maxDuration: '2m',
    },
  },
  thresholds: {
    'holds_won{scenario:hot_seats}': [`count==${HOT_SEATS.length}`],
    'holds_won{scenario:sell_out}': [`count==${ECONOMY_SEATS}`],
    server_errors: ['count==0'],
    unexpected_status: ['count==0'],
    'http_req_duration{expected_response:true}': ['p(95)<2000'],
  },
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
};

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

function holdRequest(i, body) {
  return {
    method: 'POST',
    url: `${BASE}/holds`,
    body: JSON.stringify(body),
    params: {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${TOKENS[i % TOKENS.length]}`,
        'Idempotency-Key': uuid(), // every attempt is a distinct request, not a retry
      },
    },
  };
}

function record(responses, expectedLoss) {
  for (const r of responses) {
    if (r.status === 201) won.add(1);
    else if (r.status === 409) {
      lost.add(1);
      check(r, { 'loser gets the right error code': (res) => expectedLoss.includes(res.json('code')) });
    } else if (r.status >= 500 || r.status === 0) serverErrors.add(1);
    else unexpected.add(1, { status: String(r.status) });
  }
}

export function hotSeats() {
  const seat = HOT_SEATS[__ITER];
  const batch = [];
  for (let i = 0; i < BATCH; i++) {
    batch.push(holdRequest(__VU + i, { trainNumber: TRAIN, travelDate: DATE, fareClass: 'BUSINESS', seatNo: seat }));
  }
  record(http.batch(batch), ['SEAT_UNAVAILABLE']);
}

export function sellOut() {
  const batch = [];
  for (let i = 0; i < BATCH && (__VU - 1) * BATCH + i < ANY_SEAT_REQUESTS; i++) {
    batch.push(holdRequest(__VU + i, { trainNumber: TRAIN, travelDate: DATE, fareClass: 'ECONOMY' }));
  }
  record(http.batch(batch), ['SOLD_OUT']);
}
