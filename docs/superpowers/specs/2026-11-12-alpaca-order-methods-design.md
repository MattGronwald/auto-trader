# Alpaca order methods design (WP 1.6a)

Date: 2026-11-12. Status: owner decisions in chat 2026-11-12 and 2026-11-22, recorded as PLAN
D22; this written spec is in review. Choices made here that D22 does not fix are marked
**(builder call)**.
Revised before review, twice: (1) a cold builder check showed that timeouts alone bound neither
DNS nor a call that starts late in a shared engine budget; (2) a Fable architecture review
(2026-11-22, verdict "needs changes") found that a provably unsent exit was recorded `unknown` and
never retried, and that a thread-based transport cannot stop a cancelled caller from sending or
keep hung threads from starving exits. Owner decisions on that review: async `httpx` instead of
`requests` (D22.1, revised) and provably unsent requests become `not_submitted` (D22.5, revised).
The review's other findings are folded in and marked "Fable".
Owner review of PR #41 (2026-11-22, three P2 findings, decided "go"): trading DNS shares the
default thread pool with market data (1, 3.1); a persistent unknown status had no repair path
(3.4); clients leaked on a failed or cancelled startup (3.5, 3.7). Marked "PR #41". A second
owner review (2026-11-22, one P2, decided "go") found that teardown never receives the broker at
all (3.7, "Runtime ownership"); a third (one P2, decided "go") that teardown must read the
holder only after its startup step (same section).

## 1. Scope and intent

`AlpacaBroker` today reads account, positions and asset rules through alpaca-py's
`TradingClient`, and every order method raises `OrderMethodsUnavailable`
([alpaca.py](../../../src/autotrader/broker/alpaca.py)). WP 1.6 makes Alpaca paper trading
possible. It is split into four sub-WPs (PLAN 1.6 row, owner 2026-11-12); this is the first.

1.6a delivers the adapter's order side and a trading-API transport we control:

1. **An async HTTP transport on `httpx`** for every trading-API call (D22.1), replacing
   `TradingClient`. No worker threads; cancelling or timing out the caller stops the request.
2. **`submit_order`, `cancel_order`, `get_order`, `get_order_by_client_id`** for Alpaca, with
   ack validation and one error contract mapped from the probed status codes (D22.2).
3. **Provably unsent requests are `not_submitted`** (D22.5): a failure before any byte of the
   request was written is retried by the existing paths instead of becoming an operator incident.
4. **`BrokerOrderView` gains order type, limit price and time in force**, and the resolver's
   match check compares them (PR #35 finding m2).
5. **`close_position`, `close_all` and `stream_fills` leave the `Broker` protocol** (D22.3).

What it does **not** do (later sub-WPs):

- No order reaches Alpaca from the runtime. `can_place_orders()` stays false, and the adapter
  refuses order writes on its own as well (3.8). Enabling is 1.6d.
- `order_fills` (activities API), `broker_fill_id`, fee/rounding and the 1.5b ledger hardening
  are 1.6b. `order_fills` keeps raising `OrderMethodsUnavailable`.
- Buying power, broker max notional, quote/snapshot age, the $10 buy minimum in
  `AssetRules.min_notional` and full-close `min_notional` are 1.6c.
- The market-data client (`broker/alpaca_data.py`) stays on alpaca-py, which has no HTTP timeout
  and runs in asyncio's default thread pool. Trading calls no longer use that pool for requests,
  but their DNS lookups still do: asyncio's `loop.getaddrinfo()` runs in the default executor, and
  the write client opens a fresh connection (so a fresh lookup) per write. Hung market-data calls
  that fill the pool (`min(32, cpus + 4)` workers) therefore delay a write's DNS until its 3 s
  connect budget is spent; the exit becomes `not_submitted` and is retried, but every retry meets
  the same full pool (PR #41, finding 1). Fixed in 1.6c, not here (PLAN 1.6 row): the market-data
  client gets a timeout **and its own thread pool**, so nothing but DNS uses the default executor,
  plus a test that fills the market-data pool with hung calls and shows a write still connects
  within budget. Both are preconditions of 1.6d enabling paper writes. Isolating DNS inside the
  trading transport (a custom httpcore network backend that resolves in a dedicated pool) was
  considered and not chosen: more code on the order path for what one dedicated pool also solves.
  **2026-11-22 (owner, D22 (9), after a Fable review of the plan):** a narrower backend *is*
  added, for a different reason. httpcore's `start_tls` closes the socket only on `Exception`,
  so a request cancelled during the TLS handshake left an open socket that `aclose()` cannot
  reach. The backend wraps httpcore's anyio backend, resolves nothing itself, and only closes
  the stream on cancellation. This makes the "both clients closed" claims of 3.7 and acceptance 15
  hold for that case too.

**Why own the transport.** alpaca-py 0.44's `TradingClient` is synchronous `requests` code
(the package's only async code is the websocket streams). It sends every request without a
timeout (`alpaca/common/rest.py`, `_request` builds `opts` with no `timeout`), retries 429 and 504
up to three times with 3 s sleeps, POST included, and `TradingClient.__init__` exposes neither
setting (it passes only keys, `paper`, `raw_data`, `url_override` to its base class, whose
`retry_attempts=0` is ignored anyway). Turning retries off and adding timeouts would mean writing
to private attributes that an upgrade can silently undo.

**Alternatives considered** (Fable I8):

- *Keep alpaca-py, patch its private session.* Still threads (below), plus private-API coupling.
- *Own `requests` transport in `asyncio.to_thread`* (the first version of this spec). A thread
  cannot be stopped: a caller that times out or is cancelled (the Cycle Runner force-cancels a
  `check()` still in its submit at shutdown, `core/cycle.py` `close`) leaves a worker that may still
  connect and send; DNS has no timeout in that stack; and hung threads fill asyncio's default
  executor (`min(32, cpus + 4)` workers, 5 on a 1-vCPU VPS), so an exit can wait for a thread until
  its budget is spent. Fixing that needed a deadline token, an abandon flag, a urllib3 connection
  subclass and dedicated executors.
- *Separate process for broker I/O.* Strong isolation, far more machinery than the PoC needs.
- **Chosen: `httpx.AsyncClient`.** The request runs as a coroutine on the event loop, so the
  engine's `asyncio.wait_for` (and any cancellation) stops it where it is; a request whose
  connection is not yet established is never written. The connect timeout covers DNS and TCP/TLS
  setup (DNS runs in the default thread pool, so it is bounded but not isolated; 1 above). `httpx.MockTransport` is a built-in test seam. Cost: new dependencies (`httpx` with
  `httpcore`, `anyio`, `h11`, `certifi`, `idna`).

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
`order_visibility_allowance_s`. Rate limit (Alpaca support and docs, checked 2026-11-22): **200
requests per minute per account** on the trading API, 429 above it.

## 3. Design

### 3.1 Transport (D22.1)

A small module, `broker/alpaca_http.py` **(builder call: name)**, owns two `httpx.AsyncClient`s
created by `AlpacaBroker.connect` and closed by `AlpacaBroker.aclose()` (3.7):

- **Read client** (GETs): keep-alive pool, small explicit `httpx.Limits` **(builder call: sizes)**.
- **Write client** (`POST /orders`, `DELETE /orders/{id}`): **no connection reuse**
  (`max_keepalive_connections=0`), so every write opens a fresh connection (Fable I3). A pooled
  keep-alive connection that the server or a NAT dropped silently can accept the write locally and
  then fail, turning a request Alpaca never processed into `unknown`. A fresh TLS connection costs
  about 0.1 s against a 10 s budget.
- Both: base URL from the mode (`https://paper-api.alpaca.markets/v2` for paper; live only where
  `AlpacaBroker.connect` already allows it, unchanged `ALLOW_LIVE` gate), auth headers
  `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY` set once, `httpx.Timeout(connect=3, write=5, read=5,
  pool=2)` **(builder call:** `pool`), `follow_redirects=False`, `trust_env=False` (no env proxies,
  no `.netrc`, no `SSL_CERT_FILE` override), TLS verification on and never overridden, cookies not
  kept **(builder call:** mechanism, e.g. a cookie jar whose policy accepts none) (Fable M2).
- **No retries of any kind:** the default transport (`retries=0`), no loop. A 429 or 5xx is
  returned to the caller as a failure.
- Decimals go out as plain strings (`format(d, "f")`, no exponent); responses are parsed with
  `Decimal(str)` from the JSON strings, as the read side does today.
- **One log line per call** (Fable M1): `alpaca.http` with method, path template (the order id as
  its own field), status or exception type, elapsed ms and Alpaca's `X-Request-ID`. Non-2xx
  responses add Alpaca's `message`. Never headers, never keys. The request id also goes into every
  `BrokerError` message raised from a response.
- **Request budget** (Fable I5): `asset_rules` is cached per symbol for 5 minutes **(builder
  call:** TTL). Today the 2 s sync loop calls it once per symbol per pass, and `exit()`/`check()`
  once per held symbol; uncached, three unsettled orders on three symbols already use about 180 of
  the 200 requests per minute. The probed `min_order_size` (about $1) moves with price, but the $10
  buy minimum dominates it (alpaca-notes), so a 5-minute value is safe. The plan states the
  expected steady-state and peak request rate.

`httpx` becomes a direct dependency; `requests` is not added. alpaca-py stays for market data.

### 3.2 When a request is "not sent" (D22.5)

**Invariant 1, no late send.** A request is written only while its caller is still waiting for
it. When the engine's `_within` times out (`asyncio.wait_for`) or the caller is cancelled, the
coroutine is cancelled at its current await. If the connection is not yet established (DNS, TCP,
TLS, or waiting for a pool slot), nothing is written, ever. If cancellation lands while the
request is being written, the request is cut off mid-way and the connection closed; an HTTP server
does not act on an incomplete request **(builder call:** verify against the installed httpcore
that a cancelled write closes the connection rather than finishing it). If the request was fully
written, the order may exist: `unknown`, as today. No thread outlives its caller, so this also
holds for the forced cancel at shutdown (Fable I1).

**Invariant 2, provably not sent.** The transport raises **`RequestNotSent(BrokerError)`**
(new, `broker/base.py`) when the request failed before any of it was written:

| httpx outcome | Raised | Why |
|---|---|---|
| `ConnectError` (DNS failure, refused, TLS handshake failure) | `RequestNotSent` | the connection never existed |
| `ConnectTimeout` | `RequestNotSent` | same |
| `PoolTimeout` | `RequestNotSent` | no connection was handed out |
| `WriteError`, `WriteTimeout`, `ReadError`, `ReadTimeout`, `RemoteProtocolError`, any other transport error | `BrokerError` | bytes may have reached the server |

This leans on httpcore raising `ConnectError`/`ConnectTimeout` only from connection setup.
**(builder call:** confirm in the installed httpcore that the HTTP/1.1 connection is fully
established, TLS included, before the first request byte, and that no write-phase failure is
reported as a connect error; pin `httpx`/`httpcore` to the verified minor versions and note
"re-check on upgrade" in `docs/alpaca-notes.md`.) Because the write client never reuses
connections, there is no stale-connection case in which a "connect" failure follows a write.

The engine's own timeout (`TimeoutError` from `_within`) stays `unknown`: the engine cannot see
whether the cancelled request had started writing. Only the transport's own errors carry proof.

**Engine mapping.** `_submit` (`risk/engine.py`) catches `RequestNotSent` before the generic
handler and records `not_submitted`, like `OrderMethodsUnavailable` today (log
`risk.order_not_submitted`, reason "request not sent: <exception type>"). Effects through existing
paths:

- **Exit:** `not_submitted` reaches the Position Manager's backoff and is retried
  (`position_manager.py`, the `backoff.fail` branch), which PLAN 3.2 item 5 requires for a
  "proven-unsent exit". Before this change a short DNS or connect outage during a stop-out left the
  row `unknown`, which is never retried, became an incident after 80 s, and needed a core-stopped
  repair (Fable C1).
- **Entry:** the cycle completes as the runner does for `not_submitted` today; no incident and no
  all-symbol block after a restart.
- `cancel_order` and reads: `RequestNotSent` is a `BrokerError`, so their callers treat it as a
  failed call (a failed cancel is retried with backoff; a failed read fails the pass closed).

**Resolver arithmetic** (Fable M5). An order can be created no later than the end of a write that
started before the caller's deadline: at most `submitted_at + io_timeout_s + 5 s` (write timeout)
= 15 s. The 80 s `order_resolution_timeout_s` still leaves the 60 s visibility allowance after it.

### 3.3 Error contract (D22.2)

One mapping at the transport boundary. The engine's existing handling (`_submit`, `_sync`, the
resolver) does the rest.

| Outcome | Raised | Engine result |
|---|---|---|
| failure before any byte was written (3.2) | `RequestNotSent` | submit: `not_submitted`; other calls: failed |
| write/read error or timeout, any 5xx, 429, malformed or unparsable body | `BrokerError` | submit: row `unknown` (resolver); reads: the pass fails closed |
| submit **403 with a parsable JSON body** | `OrderRejected` (status, Alpaca's `message`, request id) | row `rejected`. A 403 without a parsable body is `BrokerError` |
| submit **any other 4xx** (400, 401, 404, 422, …) | `BrokerError` | row `unknown`: the resolver looks the client id up. A 422 is also "client_order_id must be unique", which means an order with that id exists; it must be found, not recorded as rejected. A malformed order shape (a bug) becomes an operator incident after `order_resolution_timeout_s` instead of a silent rejection: deliberate. 401 and 429 stay here in 1.6a; whether they may be `not_submitted` is a 1.6d decision with paper evidence (Fable I5) |
| `get_order` **404** | `OrderNotFound` | resolver type (b), unchanged |
| `get_order` with a status 3.4 does not list | `UnknownOrderStatus(BrokerError)` carrying the raw status | resolver type (b), like `OrderNotFound` (3.4) |
| `get_order_by_client_id` **404** | returns `None` | resolver: zero match, unchanged |
| `cancel_order` **204** | returns | |
| `cancel_order` **422** (order already final, e.g. filled) | `BrokerError` (Fable M4) | `cancel_open_entries` counts it `failed` and retries with backoff until the sync books the final status and it is no longer an open entry. Returning normally would put it into the Position Manager's `_cancel_requested`, which is never retried |
| `cancel_order` **404** | `OrderNotFound` | |
| any other non-2xx | `BrokerError` | |

Never branch on Alpaca's body `code` (it was not stable across identical requests).

### 3.4 Order mapping

**Request.** `BrokerOrder` → JSON `symbol`, `side`, `type`, `qty`, `time_in_force`,
`client_order_id` when the order has one, and `limit_price` for limits. A `BrokerOrder` without a
client id is sent without one: the engine lets an exit go out without it when the journal id is
missing (logged; `risk/engine.py` docstring), so a protective exit must not be refused here.
Alpaca then assigns its own id, which the ack check below does not compare.

**Ack validation.** A 200 is accepted only if the response's `client_order_id` (when we sent
one), `symbol`, `side`, `type`, `time_in_force`, `qty` (Decimal equality) and `limit_price`
(Decimal equality, both null for market) equal the request, and `id` is a non-empty string. Any
difference raises `BrokerError`, so the row becomes `unknown` and the resolver's match check
decides with full information. Before raising, the adapter logs `alpaca.ack_mismatch` at error
level with the broker order id and each differing field (Fable M3): a live order exists, and the
operator needs its id.

**Status.** Alpaca → `OrderState`:

| Alpaca | Ours |
|---|---|
| `pending_new`, `accepted`, `new` | `new`, or `partially_filled` if `filled_qty > 0` |
| `partially_filled` | `partially_filled` |
| `pending_cancel` | `new` / `partially_filled` by `filled_qty` (not final) |
| `filled` | `filled` |
| `canceled`, `expired` | `canceled` (final; `expired` documented, not observed) |
| `rejected` | `rejected` |
| anything else (`done_for_day`, `replaced`, `pending_replace`, `stopped`, `suspended`, `calculated`, `held`, `accepted_for_bidding`, unknown values) | `UnknownOrderStatus`: we never place orders that should reach them; fail closed rather than guess |

**An unknown status has a bounded path** (Fable I6). Today a `get_order` that raises anything but
`OrderNotFound` fails the row on every 2 s pass, forever, and the row never becomes a resolver
candidate (`engine.py` `_read`). `_read` catches `UnknownOrderStatus` like `OrderNotFound`: the
row becomes a type (b) candidate (clock `resolve_since`), logged `risk.order_status_unknown` with
the raw status. The resolver's client-id lookup meets the same status; that outcome is recorded
as the new incident reason **`unknown_status`** (fits `orders.incident`, `String(24)`, no
migration), so after `order_resolution_timeout_s` the row is an incident with the existing
reminder.

**Recovering an `unknown_status` incident** (PR #41, finding 2). The existing commands could not:
`repair link` turns any lookup exception into a refusal (`core/repair.py` `prepare_link`), and
`repair not-submitted` refuses rows with a broker id. No new command; instead:

- `unknown_status` joins `LINKABLE_INCIDENTS`.
- For an `unknown_status` row, `repair link` additionally requires the broker order to be
  **final** (`filled`, `canceled` or `rejected` after mapping), else it refuses ("broker order is
  not final: <status>"). The usual checks (ownership by client id, `mismatches`, fresh lookup,
  conditional commit) are unchanged. Requiring a final state keeps a row from being re-linked
  while the order can still move, and from bouncing between linked and incident.
- Runbook procedure (stop the core first, as for every repair): find the order at the broker by
  its broker id; bring it to a final state (cancel it in the Alpaca dashboard, or wait for it to
  finish); then `repair link <row> <same broker id>`. While Alpaca keeps the order in an unmapped
  state (`held`, `suspended`, ...), escalate to Alpaca support; meanwhile the row stays an incident
  and keeps its symbol blocked, the fail-closed outcome. The ledger books the row's fills from the
  final state on the next pass after restart.

`OrderAck.submitted_at` is the response's `submitted_at` (else `created_at`); `OrderStatus.reason`
is null. `avg_fill_price` from `filled_avg_price` (null until filled). Symbols map through the
existing `_slash_symbol` (orders use `BTC/USD`, positions `BTCUSD`).

### 3.5 Reads on the new transport

`get_account`, `get_positions`, `asset_rules` and the `connect` checks (`crypto_status`,
`crypto_tier`) move to the read client with their current parsing unchanged; `RawTradingClient`,
`_make_client` and `_call` (the `to_thread` wrapper) go. **2026-11-22 correction (plan dry
run):** `_call` does not go: the market-data client (`broker/alpaca_data.py`) still uses it, so
it moves there unchanged until 1.6c. Startup's broker check
(`core/runtime.py` `_check_broker`: `connect()` then `get_account()`) gets one overall timeout of
`io_timeout_s` **(builder call)**, so a hung connect no longer blocks startup unbounded (Fable M7).
Both steps close the clients on failure (3.7).

### 3.6 Match check: `BrokerOrderView` and `mismatches`

`BrokerOrderView` gains `type: OrderType`, `limit_price: Dec | None`, `time_in_force:
TimeInForce`. `mismatches` (`risk/resolve.py`) compares `type` and `limit_price` (Decimal
equality, both null for market) against the order row, which stores both.

The row has **no `time_in_force` column**. **(builder call:** derive it, no migration: the engine
sends exactly two shapes, entry `limit`/`ioc` and exit `market`/`gtc`; one function next to
`mismatches` maps the row's `type` to the expected TIF, and a row whose type is neither is a
mismatch.) `repair link` gets the same comparison through `mismatches`. SimBroker's
`get_order_by_client_id` fills the new fields from the order it stored.

### 3.7 Lifecycle

`AlpacaBroker.aclose()` closes both clients. The runtime's teardown calls it (below);
`cli_repair` calls it in a `finally`, so a refused or failed repair closes it too. SimBroker gets
a no-op `aclose()` and the `Broker` protocol gains it **(builder call:** or the runtime calls it
only when present).

**Startup ownership** (PR #41, finding 3). Teardown can close only a broker it was handed, so each
handoff before that closes what it created on any failure, cancellation included
(`BaseException`, re-raised):

- `AlpacaBroker.connect()` creates both clients, then validates the account (`crypto_status`,
  `crypto_tier`). If validation fails or is cancelled, it closes both clients before re-raising.
- `_check_broker` (`core/runtime.py`) receives the broker from `connect()`, then calls
  `get_account()` under the startup timeout. If that fails, times out or is cancelled, it calls
  `aclose()` before re-raising. Only a broker that passed both steps reaches the runtime's
  teardown.

**Runtime ownership** (PR #41, second review). Today `start()` (`core/runtime.py` `run_core`)
keeps the broker in a local and `_Started` does not carry it, so `_teardown` never sees it on any
path. After `_check_broker` returns, `start()` can still lose it: the mode-mismatch raise, a stop
during the backfill (teardown cancels the unfinished startup task), the `return None` after a
stop request, or any later startup error. So:

- `run_core` keeps a `broker` holder next to `supervisor` and `runner`. `start()` assigns it in
  the statement right after `_check_broker` returns, with no `await` in between; from then on the
  holder owns the broker. Ownership is one chain: `connect()`, then `_check_broker`, then the
  holder.
- `_teardown` takes the broker and closes it as its own bounded step `broker`, after the runner,
  the Position Manager (supervisor), the stream and the bus, so no exit or cancel still in flight
  loses its client. Like every step, a forced or failed close makes the exit unclean.
- `_teardown` receives the **holder (or an accessor), not its value**, and reads it only in the
  `broker` step (PR #41, third review). A stop can win the startup race while `_check_broker` is
  still in `get_account()`; the value at call time is then `None`, but teardown's first step waits
  for the unfinished startup, which can finish the check, fill the holder and return `None`
  because a stop was requested.
- If the `startup` step itself runs out of budget, teardown cancels it once and leaves it (forced,
  so the exit is already unclean). The cancellation lands either before the holder is filled,
  where `connect()` / `_check_broker` close the clients in their own cleanup (best effort, not
  awaited by teardown), or after, where the `broker` step closes it.

### 3.8 Writes stay off (defence in depth)

Callers gate on `can_place_orders()` today (Position Manager, runtime admission), but `exit()`,
`check()` and `cancel_open_entries()` themselves do not. So the adapter also refuses:
`AlpacaBroker` takes `orders_enabled: bool = False`; while false, `submit_order` and
`cancel_order` raise `OrderMethodsUnavailable` before any I/O (the engine records
`not_submitted`, as today), and `can_place_orders()` returns `orders_enabled`.

- **Paper only from day one** (Fable I7): the constructor raises if `orders_enabled` is true and
  the account is not paper. In 1.6a nothing outside tests sets it: `connect()` has no parameter or
  setting that can. 1.6d decides how paper enabling works; live writes stay impossible before
  Phase 5 (PLAN 1.6 row).
- `get_order` and `get_order_by_client_id` are always available, so `repair link` works against
  Alpaca from 1.6a on.

### 3.9 Protocol cleanup (D22.3), SimBroker

Remove `close_position`, `close_all` and `stream_fills` from `Broker`, from `AlpacaBroker`, and
from SimBroker unless a remaining SimBroker test needs them. Test helpers that use
`sim.close_position(symbol)` to create a sell switch to `submit_order` of the position's
`qty_available` with a market sell (what `exit()` does since D20). SimBroker's special case that
lets an off-increment sell through only via `close_position` moves with it **(builder call:** keep
the behaviour for a full-holding sell). `redeliver` and its tests go with `stream_fills` unless a
ledger test depends on the redelivery mechanism, in which case it is re-expressed through
`order_fills`.

SimBroker's `cancel_order` follows the Alpaca contract **(builder call**, from the builder
check): an already-canceled order returns (Alpaca 204); a filled or rejected one raises
`BrokerError` (Alpaca 422, 3.3); an unknown id raises `OrderNotFound`. Engine tests are adjusted
so they model Alpaca.

Land the protocol cleanup and the SimBroker changes as their own commit(s), ahead of the
transport, so the transport diff is reviewable alone (Fable M8).

### 3.10 Tests

No test touches the network (CLAUDE.md). Adapter tests use `httpx.MockTransport` with fixtures
from `tests/fixtures/alpaca/order_probes_20261112.json`.

Acceptance (all against `MockTransport` unless noted):

1. Each row of 3.3 raises or returns exactly as specified, for each method.
2. Through the engine: a 422 "client_order_id must be unique" on submit leaves the row `unknown`;
   a 403 `rejected`; `ConnectError`, `ConnectTimeout` and `PoolTimeout` `not_submitted`;
   `WriteError`, `ReadTimeout` and `RemoteProtocolError` `unknown`.
3. Position Manager: an exit whose submit raises `RequestNotSent` is retried on a later step after
   its backoff, and the retry can succeed.
4. Ack validation: each of the seven fields, changed alone, raises `BrokerError` and logs
   `alpaca.ack_mismatch` with the broker id; the recorded probe acks pass unchanged.
5. Every status in the 3.4 table maps as listed; an unlisted status raises `UnknownOrderStatus`,
   and through `sync_orders` the row becomes a type (b) candidate and, after the resolution
   timeout, an `unknown_status` incident. `repair link` on that row refuses while the lookup still
   raises and while the broker order is not final, and links it once the order is final.
6. No retries: a 429, a 503 and a `ReadTimeout` each reach the mock handler exactly once.
7. Clients carry the specified timeouts, `trust_env=False`, no redirects, no keep-alive on the
   write client; secrets appear in no log line and no exception message (captured logs and
   `str(exc)`); each call logs `alpaca.http` with the request id.
8. Decimals are sent without exponent (`Decimal("1E-7")` → `"0.0000001"`).
9. **No late send** (Invariant 1), deterministic, events only, no sleeps: a handler that blocks on
   an event, with the caller cancelled or timed out by `wait_for` before the event is set, records
   no completed request. Plus one loopback test through the real `httpx` stack: an `asyncio` server
   on 127.0.0.1 that records received bytes, the connection held in setup by the test, the caller
   cancelled: zero bytes arrive **(builder call:** how to hold the connection in setup
   deterministically).
10. `orders_enabled=False`: `submit_order` and `cancel_order` raise `OrderMethodsUnavailable` with
    no request; `can_place_orders()` is false; `get_order` / `get_order_by_client_id` still work.
    `orders_enabled=True` with a live account raises in the constructor.
11. `mismatches`: type, limit price and derived TIF differences are reported; the engine resolver
    and `repair link` turn them into `client_id_mismatch` / a refusal.
12. `asset_rules` is served from the cache within the TTL and refetched after it.
13. The three removed methods are gone from the protocol; mypy proves no caller remains.
14. SimBroker `cancel_order`: canceled returns, filled raises `BrokerError`, unknown raises
    `OrderNotFound`.
15. Startup cleanup: with a `MockTransport` handler that blocks, cancelling `connect()` during its
    account check, and failing or cancelling `_check_broker`'s `get_account()`, each leaves both
    clients closed (`is_closed`); a failed `crypto_status` check does too. Through `run_core`:
    a shutdown requested while the backfill blocks, a shutdown requested while `get_account()`
    blocks and the check then succeeds within the shutdown budget, a broker/config mode mismatch,
    and a clean shutdown each leave both clients closed; a refused `repair link` closes them in `cli_repair`.
16. Opt-in `smoke` (read-only, places no orders): account, positions and asset rules through the
    new transport; `get_order` of a random UUID raises `OrderNotFound`; `get_order_by_client_id` of
    an unknown id returns `None`.

## 4. Docs (same PR as the change)

Claims this falsifies or narrows, to correct when the code lands:

- `broker/alpaca.py` module docstring (alpaca-py `raw_data`, `to_thread`, "order submission lands
  in 1.6").
- `broker/base.py`: `OrderMethodsUnavailable` docstring ("Alpaca until WP 1.6": writes until
  1.6d, `order_fills` until 1.6b); the `Broker` protocol; the new `RequestNotSent` and
  `UnknownOrderStatus`.
- `risk/engine.py` module docstring: the mention of `close_position`; `not_submitted` now also
  covers a provably unsent request; `_read` for an unknown status.
- SPEC §4.6 interface listing and "`AlpacaBroker` is currently read-only"; SPEC §5 step 6 "fill
  stream" (fills are pulled, D17.3).
- `docs/runbook.md`: lines saying `repair link` / client-id lookup is unavailable until 1.6 (now
  available); "cannot place orders until WP 1.6" → 1.6d; the `unknown_status` incident and its
  recovery procedure (3.4), `unknown_status` in the `repair link` list of linkable incidents; the
  `alpaca.http` log line and where the request id is.
- `docs/alpaca-notes.md`: the transport facts (alpaca-py has no timeout and retries POST; trading
  calls no longer use it), the rate limit, and "re-check on httpx/httpcore upgrade".
- SimBroker docstrings (cancel contract, removed methods).

## 5. Left to the builder

Module and helper names; pool sizes and the pool timeout; the cookie mechanism; the cache TTL; the
startup timeout; whether `aclose` joins the protocol; the loopback test mechanics; how SimBroker
test helpers are rewritten. Everything marked **(builder call)** may be decided differently with a
one-line reason in the plan.
