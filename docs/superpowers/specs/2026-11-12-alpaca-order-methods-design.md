# Alpaca order methods design (WP 1.6a)

Date: 2026-11-12. Status: owner decisions in chat 2026-11-12, recorded as PLAN D22; this written
spec is in review. Choices made here that D22 does not fix are marked **(builder call)**.
Revised 2026-11-12 after the cold builder check (before review): the send bound in 3.1 was wrong
(shared engine budget, unbounded DNS, the request is written under the connect timeout); owner
chose a no-send-after-deadline guard (D22.5, 3.2). SimBroker's cancel follows the Alpaca contract
and the Session is shared (both builder calls, 3.1, 3.8).

## 1. Scope and intent

`AlpacaBroker` today reads account, positions and asset rules through alpaca-py's
`TradingClient`, and every order method raises `OrderMethodsUnavailable`
([alpaca.py](../../../src/autotrader/broker/alpaca.py)). WP 1.6 makes Alpaca paper trading
possible. It is split into four sub-WPs (PLAN 1.6 row, owner 2026-11-12); this is the first.

1.6a delivers the adapter's order side and a trading-API transport we control:

1. **Our own HTTP layer** for every trading-API call (D22.1), replacing `TradingClient`, that
   never starts a send after the engine has given up on the call (D22.5).
2. **`submit_order`, `cancel_order`, `get_order`, `get_order_by_client_id`** for Alpaca, with
   ack validation and one error contract mapped from the probed status codes (D22.2).
3. **`BrokerOrderView` gains order type, limit price and time in force**, and the resolver's
   match check compares them (PR #35 finding m2).
4. **`close_position`, `close_all` and `stream_fills` leave the `Broker` protocol** (D22.3).

What it does **not** do (later sub-WPs):

- No order reaches Alpaca from the runtime. `can_place_orders()` stays false, and the adapter
  refuses order writes on its own as well (3.7). Enabling is 1.6d.
- `order_fills` (activities API), `broker_fill_id`, fee/rounding and the 1.5b ledger hardening
  are 1.6b. `order_fills` keeps raising `OrderMethodsUnavailable`.
- Buying power, broker max notional, quote/snapshot age, the $10 buy minimum in
  `AssetRules.min_notional` and full-close `min_notional` are 1.6c.
- The market-data client (`broker/alpaca_data.py`) stays on alpaca-py. It has the same missing
  HTTP timeout; that is noted, not fixed here.

**Why own the transport.** alpaca-py 0.44 sends every request without a timeout
(`alpaca/common/rest.py`, `_request` builds `opts` with no `timeout`) and retries 429 and 504 up to
three times with 3 s sleeps, POST included; `retry_attempts=0` is ignored (only values > 0 are
taken). The engine bounds each call with `asyncio.wait_for` around `asyncio.to_thread`, which
stops waiting but not the thread: the request can still be sent, or sent again, after the engine
has recorded `unknown`. PLAN 3.2 item 4 requires a transport deadline that actually ends the work
and makes "the original worker is quiescent (no later local send/retry)" true.

## 2. Evidence (paper probes, 2026-11-12)

Two owner-run probe runs, one sample per symbol (BTC/USD, ETH/USD, SOL/USD); full table in
`docs/alpaca-notes.md` ("Order endpoint probes (WP 1.6a)"), raw responses with order ids replaced
in `tests/fixtures/alpaca/order_probes_20261112.json`. What this design relies on:

| Call | Observed |
|---|---|
| `POST /v2/orders`, accepted | 200, `status` `pending_new`; `qty`, `limit_price`, `type`, `time_in_force`, `client_order_id`, `symbol` (`BTC/USD`, with slash) echoed exactly |
| same `client_order_id` again | **422**, "client_order_id must be unique", no order in the body; body `code` differed by symbol |
| buy under $10 at its limit price | **403** "cost basis must be >= minimal amount of order 10" |
| `GET /v2/orders/{id}`, `DELETE /v2/orders/{id}`, `GET /v2/orders:by_client_order_id` for an unknown id | **404** |
| `DELETE` a resting order | **204**; next read `canceled`; `DELETE` again **204** |
| `DELETE` a filled order | **422** "order is already in "filled" state" |
| non-marketable IOC limit | final `canceled`, zero fill |
| lookup by client id after submit | found on the first lookup, 0.22–0.30 s after sending |

Bounded: one sample per symbol, paper only. The visibility bound stays the configured 60 s
`order_visibility_allowance_s`.

## 3. Design

### 3.1 Transport (D22.1)

A small module, `broker/alpaca_http.py` **(builder call: name)**, owns a `requests.Session`:

- Base URL from the mode: `https://paper-api.alpaca.markets/v2` for paper, the live URL only when
  `AlpacaBroker.connect` already allowed live (unchanged `ALLOW_LIVE` gate). Auth headers
  `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY` set once on the session; never logged, never in an
  exception message.
- Every request: `timeout=(3, 5)` (connect, read), `allow_redirects=False`, **no retries** of any
  kind (no urllib3 `Retry`, no loop). A 429 or 5xx is returned to the caller as a failure.
- **What the timeouts alone do not bound** (urllib3 2.x, read, not run): the DNS lookup
  (`getaddrinfo`) has no timeout; the connect timeout applies per resolved address (IPv4 + IPv6 can
  take 2 x 3 s); the request is written under the connect timeout. And the engine budget is shared
  per locked section (`check()` spends it on the gate and several reads before `submit_order`), so
  a call may start with 1-2 s left. Without 3.2, a thread could send after the engine recorded
  `unknown`, by an unbounded amount. Without retries the request is still sent **at most once**.
- One `requests.Session` per `AlpacaBroker`, shared by the worker threads, with headers set once
  and never mutated afterwards and an explicitly sized connection pool **(builder call)**:
  concurrent calls (entries, exits, the sync, snapshots, the cash check) then share only urllib3's
  thread-safe pool.
- Calls run in `asyncio.to_thread`, as today. The engine's `_within` stays the outer budget.
- **(builder call)** whether to also enforce a total read deadline; the read side cannot create an
  order, so 3.2 does not need it.
- Decimals go out as plain strings (`format(d, "f")`, no exponent); responses are parsed with
  `Decimal(str)` from the raw JSON strings, as the read side does today.

`requests` becomes a direct dependency (it is already installed through alpaca-py);
`types-requests` a dev dependency for mypy. alpaca-py stays for market data.

### 3.2 No send after the deadline (D22.5)

Invariant: **no request starts being written after the caller's deadline.** A write that started in
time finishes or fails within the connect timeout (3 s). Hence, once the engine's `_within` has
raised `TimeoutError` and the row is `unknown`, the worker can no longer create an order later
than about 3 s after that deadline, however long DNS, connect or TLS took. This is the "original
worker is quiescent" condition of PLAN 3.2 item 4, and the "one total transport deadline per call
within the engine I/O budget" of the PLAN 1.6 row.

- **Deadline handoff, no protocol change.** `_within` (`risk/engine.py`) sets a context variable to
  its absolute monotonic deadline for the duration of the call. `asyncio.to_thread` runs the
  function in a copy of the current context, so the transport reads it in the worker thread. The
  variable lives in a small shared module both sides import **(builder call:** module and name;
  `broker/` must not import `risk/`).
- **Fallback.** A call without a deadline in context (`AlpacaBroker.connect`, `cli_repair`'s
  lookup, tests) uses its own start + 10 s **(builder call:** the value; same as the engine's
  default `io_timeout_s`).
- **Where the check sits.** After the connection is usable (DNS, TCP and TLS done, or a pooled
  connection taken) and immediately before the request is written: a urllib3 connection subclass
  (`request()` override) installed through a `requests` `HTTPAdapter` **(builder call:** exact hook;
  it must sit after connect, not before the call starts). Past the deadline it raises a dedicated
  error and writes nothing; the transport maps it to `BrokerError` (the engine already timed out,
  so the row is `unknown` either way). Reads get the same check (no harm, one code path).
- The same monotonic clock as the engine (`time.monotonic`) on both sides.

### 3.3 Error contract (D22.2)

One mapping at the transport boundary. The engine's existing handling (`_submit`, `_sync`, the
resolver) does the rest unchanged.

| Outcome | Raised | Engine result |
|---|---|---|
| connect error, timeout, connection reset, any 5xx, 429, malformed or unparsable body | `BrokerError` | submit: row `unknown` (resolver); reads: the pass fails closed |
| submit **403** | `OrderRejected` (message: status and Alpaca's `message`) | row `rejected` |
| submit **any other 4xx** (400, 401, 404, 422, …) | `BrokerError` | row `unknown`: the resolver looks the client id up. A 422 is also "client_order_id must be unique", which means an order with that id exists; it must be found, not recorded as rejected. A malformed order shape (a bug) becomes an operator incident after `order_resolution_timeout_s` instead of a silent rejection: deliberate |
| `get_order` **404** | `OrderNotFound` | resolver type (b), unchanged |
| `get_order_by_client_id` **404** | returns `None` | resolver: zero match, unchanged |
| `cancel_order` **204** | returns | |
| `cancel_order` **422** | returns **(builder call)**: the order is already final; the next sync read books its real status. Logged at info | |
| `cancel_order` **404** | `OrderNotFound` | |
| any other non-2xx | `BrokerError` | |

Never branch on Alpaca's body `code` (it was not stable across identical requests). Messages carry
the HTTP status and Alpaca's `message`, never headers.

### 3.4 Order mapping

**Request.** `BrokerOrder` → JSON `symbol`, `side`, `type`, `qty`, `time_in_force`,
`client_order_id` when the order has one, and `limit_price` for limits. A `BrokerOrder` without a
client id is sent without one: the engine lets an exit go out without it when the journal id is
missing (logged; `risk/engine.py` docstring), so a protective exit must not be refused here.
Alpaca then assigns its own id, which the ack check below does not compare.

**Ack validation.** A 200 is accepted only if the response's `client_order_id` (when we sent
one), `symbol`, `side`, `type`, `time_in_force`, `qty` (Decimal equality) and `limit_price`
(Decimal equality, both null for market) equal the request. Any difference raises `BrokerError`: the row becomes `unknown` and
the resolver's match check decides with full information. `id` must be a non-empty string.

**Status.** Alpaca → `OrderState`:

| Alpaca | Ours |
|---|---|
| `pending_new`, `accepted`, `new` | `new`, or `partially_filled` if `filled_qty > 0` |
| `partially_filled` | `partially_filled` |
| `pending_cancel` | `new` / `partially_filled` by `filled_qty` (not final) |
| `filled` | `filled` |
| `canceled`, `expired` | `canceled` (final; `expired` documented, not observed) |
| `rejected` | `rejected` |
| anything else (`done_for_day`, `replaced`, `pending_replace`, `stopped`, `suspended`, `calculated`, `held`, `accepted_for_bidding`, unknown values) | `BrokerError`: we never place orders that should reach them; fail closed rather than guess |

`OrderAck.submitted_at` is the response's `submitted_at` (else `created_at`); `OrderStatus.reason`
is null (Alpaca's order body has no rejection reason field we rely on). `avg_fill_price` from
`filled_avg_price` (null until filled). Symbols map through the existing `_slash_symbol` (orders
already use `BTC/USD`; positions use `BTCUSD`).

### 3.5 Reads on the new transport

`get_account`, `get_positions`, `asset_rules` and the `connect` checks (`crypto_status`,
`crypto_tier`) move to the transport with their current parsing unchanged; `RawTradingClient`
and `_make_client` go. Their tests switch to the fake transport (3.9).

### 3.6 Match check: `BrokerOrderView` and `mismatches`

`BrokerOrderView` gains `type: OrderType`, `limit_price: Dec | None`, `time_in_force:
TimeInForce`. `mismatches` (`risk/resolve.py`) compares `type` and `limit_price` (Decimal
equality, both null for market) against the order row, which stores both.

The row has **no `time_in_force` column**. **(builder call:** derive it, no migration: the engine
sends exactly two shapes, entry `limit`/`ioc` and exit `market`/`gtc`; one function next to
`mismatches` maps the row's `type` to the expected TIF, and a row whose type is neither is a
mismatch.) `repair link` gets the same comparison through `mismatches`. SimBroker's
`get_order_by_client_id` fills the new fields from the order it stored.

### 3.7 Writes stay off (defence in depth)

Callers gate on `can_place_orders()` today (Position Manager, runtime admission), but `exit()`,
`check()` and `cancel_open_entries()` themselves do not. So the adapter also refuses:
`AlpacaBroker` takes `orders_enabled: bool = False`; while false, `submit_order` and
`cancel_order` raise `OrderMethodsUnavailable` before any I/O (the engine records
`not_submitted`, as today), and `can_place_orders()` returns `orders_enabled`. Nothing in 1.6a sets
it true outside tests; 1.6d decides how it is enabled (paper only). `get_order` and
`get_order_by_client_id` are always available, so `repair link` works against Alpaca from 1.6a on.

### 3.8 Protocol cleanup (D22.3)

Remove `close_position`, `close_all` and `stream_fills` from `Broker`, from `AlpacaBroker`, and
from SimBroker unless a remaining SimBroker test needs them. Test helpers that use
`sim.close_position(symbol)` to create a sell switch to `submit_order` of the position's
`qty_available` with a market sell (what `exit()` does since D20). SimBroker's special case that
lets an off-increment sell through only via `close_position` moves with it **(builder call:** keep
the behaviour for a full-holding sell). `redeliver` and its tests go with `stream_fills` unless a
ledger test depends on the redelivery mechanism, in which case it is re-expressed through
`order_fills`.

SimBroker's `cancel_order` follows the Alpaca contract **(builder call**, from the builder check):
today it raises `OrderRejected` for an order that is already final, where Alpaca answers 204
(already canceled) or 422 (filled), which 3.3 maps to a normal return. Cancelling a final order
returns normally; an unknown id raises `OrderNotFound`. Engine tests that expect `failed` from
`cancel_open_entries` for an already-final entry are adjusted, so they model Alpaca.

### 3.9 Tests

No test touches the network (CLAUDE.md). The transport takes an injectable "send" seam
**(builder call:** a small fake object or a mocking library such as `responses`; a new dev
dependency only if it saves real code). Fixtures come from
`tests/fixtures/alpaca/order_probes_20261112.json`.

Acceptance (all against the fake transport unless noted):

1. Each status code row of 3.3 raises or returns exactly as specified, for each method.
2. A 422 "client_order_id must be unique" on submit is `BrokerError`, and through the engine the
   row is `unknown`, not `rejected`; a 403 is `rejected`.
3. Ack validation: each of the seven fields, changed alone, raises `BrokerError`; the recorded
   probe acks pass unchanged.
4. Every status in the 3.4 table maps as listed; an unlisted status raises.
5. No retries: a 429, a 503 and a read timeout each cause exactly one send.
6. Requests carry `timeout=(3, 5)` and no redirects; secrets appear in no log line and no
   exception message (assert on captured logs and `str(exc)`).
7. Decimals are sent without exponent (`Decimal("1E-7")` → `"0.0000001"`).
8. `orders_enabled=False`: `submit_order` and `cancel_order` raise `OrderMethodsUnavailable` with
   no send; `can_place_orders()` is false; `get_order` / `get_order_by_client_id` still work.
9. `mismatches`: type, limit price and derived TIF differences are reported; the engine resolver
   and `repair link` turn them into `client_id_mismatch` / a refusal.
10. The three removed methods are gone from the protocol; mypy proves no caller remains.
11. Deadline guard (3.2), with the guard's clock and the connection hook driven by the test:
    a request whose connection becomes usable after the deadline (slow DNS/connect simulated) is
    never written and raises `BrokerError`; one usable before the deadline is written once; the
    context variable set by `_within` reaches the worker thread through `asyncio.to_thread`; a
    call without a context deadline uses the fallback. Engine level: a `submit_order` that starts
    with little budget left and whose connect outlasts it leaves the row `unknown` and nothing is
    written afterwards.
12. SimBroker `cancel_order` of a filled or canceled order returns; of an unknown id raises
    `OrderNotFound`.
13. Opt-in `smoke` (read-only, places no orders): account/positions/asset rules through the new
    transport; `get_order` of a random UUID raises `OrderNotFound`; `get_order_by_client_id` of an
    unknown id returns `None`.

## 4. Docs (same PR as the change)

Claims this falsifies or narrows, to correct when the code lands:

- `broker/alpaca.py` module docstring (alpaca-py `raw_data`, "order submission lands in 1.6").
- `broker/base.py`: `OrderMethodsUnavailable` docstring ("Alpaca until WP 1.6": writes until
  1.6d, `order_fills` until 1.6b); the `Broker` protocol.
- SPEC §4.6 interface listing and "`AlpacaBroker` is currently read-only"; SPEC §5 step 6 "fill
  stream" (fills are pulled, D17.3).
- `docs/runbook.md`: lines saying `repair link` / client-id lookup is unavailable until 1.6 (now
  available); "cannot place orders until WP 1.6" → 1.6d.
- `risk/engine.py` module docstring: the mention of `close_position`, and the budget paragraph
  ("Each locked section has ONE budget ...") gains the deadline handoff (3.2); `_within`'s
  docstring likewise. SimBroker docstrings.
- `docs/alpaca-notes.md`: the transport facts (no SDK timeout/retry) and that trading calls no
  longer use alpaca-py.

## 5. Left to the builder

Module and helper names; the fake-transport mechanism; how the total read deadline is enforced if
at all (3.1); cancel-422 log level; how SimBroker test helpers are rewritten (3.8). Everything
marked **(builder call)** above may be decided differently with a one-line reason in the plan.
