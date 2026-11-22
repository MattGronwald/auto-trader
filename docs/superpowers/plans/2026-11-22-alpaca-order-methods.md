# Alpaca Order Methods (WP 1.6a) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `AlpacaBroker` talks to the trading API through our own async `httpx` transport with
fixed timeouts and no retries. It implements `submit_order`, `cancel_order`, `get_order` and
`get_order_by_client_id` with one error contract. Provably unsent requests become
`not_submitted`, an unknown Alpaca status gets a bounded resolver path, and the match check
compares order type, limit price and time in force. Writes stay switched off
(`orders_enabled=False`).

**Architecture:** A new `broker/alpaca_http.py` owns two `httpx.AsyncClient`s: reads keep
connections alive, writes open a fresh connection every time. It turns transport failures into
`RequestNotSent` (nothing was written) or `BrokerError` (bytes may have been written) and logs
one `alpaca.http` line per call. A thin httpcore network backend closes the socket when a
request is cancelled during the TLS handshake. httpcore itself does not, so without it the
socket leaks (Fable plan review C1, owner decision D22 (9)). `broker/alpaca.py` keeps the endpoint semantics: the status-code
contract, order and status mapping, ack validation, the `asset_rules` cache and the
`orders_enabled` switch. The engine maps `RequestNotSent` to `not_submitted` and
`UnknownOrderStatus` to a type (b) resolver candidate with the new incident reason
`unknown_status`. The runtime owns the broker's clients through a holder and closes them in its
own teardown step. The protocol cleanup (D22.3) and the SimBroker changes land first, so the
transport diff can be reviewed on its own (spec 3.9, Fable M8).

**Tech Stack:** Python 3.12, uv, httpx 0.28 / httpcore 1.0 (new), pydantic v2, structlog,
SQLAlchemy 2 async, pytest (asyncio_mode=auto, `--strict-markers`, `filterwarnings=error`),
hypothesis, ruff, mypy strict.

**Spec:** `docs/superpowers/specs/2026-11-12-alpaca-order-methods-design.md`. Read it first;
section numbers below refer to it. Decisions: PLAN.md D22 (1)-(8).

**Status:** written 2026-11-22. The owner reviews it before the dry run; the dry run (agent in
an isolated worktree, every task verbatim) comes after that review, then the plan PR.
**Revised 2026-11-22 after a Fable review of this plan** (verdict "ready with fixes").
- The review verified the four httpcore facts against the httpcore 1.0.9 / httpx 0.28.1 source
  and by experiment, and reproduced a socket leak on a cancel during TLS setup (C1).
- Owner decisions:
  - C1 is fixed with a closing network backend (D22 (9), spec dated note in 3.1).
  - `alpaca.http` logs every 2xx at `info`.
- Every other finding (I1-I8, M1-M12) is folded in and marked "review" where it changed a
  task.

**Revised 2026-11-23 after the owner's Codex review of PR #42** (three P2s, owner "go"; marked
"Codex"):
1. No `BrokerError` raised by the adapter chains its cause (`from None`), and Alpaca's response
   text is sanitised.
2. The request budget is corrected, with a measured-headroom precondition for 1.6d at 80 % of
   the limit.
3. An unmapped status from the direct read is no longer downgraded by an inconclusive lookup.

Second Codex round (2026-11-23, two P2s, owner "go"; marked "Codex r2"):
- Every response-derived value that reaches a log or exception text goes through
  `AlpacaHttp.diag`, not only `message`.
- The `unknown_status` evidence lasts the whole resolution window, kept in memory (owner).

**Dry run 2026-11-22** (agent in an isolated worktree, branch `dryrun-1.6a`, every task as
written). Result:
- All 8 tasks green: 1409 passed, `risk/` 100 % branch coverage after every task that touched
  it.
- The loopback file passed 5/5 twice and the full suite 2/2.
- Facts 1-5 and every condition held for httpx 0.28.1, httpcore 1.0.9, anyio 4.16.0 and
  h11 0.16.0.
- A mutation check confirmed the TLS-cancel test: removing the closing backend makes both of
  its cases fail.

Its findings are folded in and marked "dry run":
- **One hard defect.** `_call` is still imported by `alpaca_data.py` (Task 5).
- **One contradiction**, resolved by the owner: `OrderNotFound` now carries the request id.
- **About 25 sketch details** (helpers, fixtures, wrong "Expected" lines, Task 8 doc lines).

Owner decisions on the result:
- `OrderNotFound` from a response carries the request id.
- `anyio>=4.16,<5` becomes a direct dependency.
- A docstring warns that a test transport serves both clients.
- PLAN gets a 1.6d note on client-id echoing mocks.

**Which code blocks are binding.** Complete test functions (Tasks 1, 2, 4, 5) are normative:
the dry run writes them as they stand. Blocks with `...` or comment bodies (Tasks 3, 6, 7) are
sketches. Their test names and assertions are binding; their setup uses the helpers the target
file already has, which each task names.

## Global Constraints

- Code is ASCII only (no `−`, `×`, `→` in `src/` or `tests/`; ruff RUF001-003).
- Per task, check in this order: `uv run ruff format`, then `uv run ruff check --fix`, then
  `uv run mypy`, then `uv run pytest`. `risk/` keeps 100 % branch coverage
  (`uv run pytest --cov=autotrader.risk --cov-branch --cov-report=term-missing tests/unit/risk`).
- Every commit is green on its own. A signature change that spans files lands in one task.
- No `assert` in `src/` (S101). Tests never reach the network: `httpx.MockTransport`, or an
  asyncio server on `127.0.0.1` started by the test. No wall-clock timing asserts; drive the
  test with events.
- Close subprocess pipes and sockets in tests (`filterwarnings=error` turns
  `ResourceWarning` into a failure).
- Commit trailer, exactly: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Timeouts (spec 3.1): `httpx.Timeout(connect=3.0, write=5.0, read=5.0, pool=2.0)`.
  `follow_redirects=False`, `trust_env=False`, TLS verify on, no retries (`retries=0`, no
  loop), no cookies kept.
  - `trust_env=False` and `verify=True` go to the hand-built transports too. An
    `AsyncHTTPTransport` has its own `trust_env=True` and would load `SSL_CERT_FILE` /
    `SSL_CERT_DIR` even under a `trust_env=False` client (review I2).
  - The connect timeout applies to TCP (with DNS) and to TLS separately, so a `RequestNotSent`
    can take up to 6 s, still inside the 10 s engine budget (review).
- Test credentials: use the module constants `KEY` / `SECRET`, never inline `"s"` (ruff S106).
  Use no single-letter `l` variables (E741) (review I5).
- Base URLs: paper `https://paper-api.alpaca.markets/v2`, live `https://api.alpaca.markets/v2`
  (live only through the existing `ALLOW_LIVE` gate in `connect`, unchanged).
- Headers `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY` are set once per client. Headers or keys
  never appear in a log line or an exception message. **Rendered** log lines count too
  (Codex P2-1):
  - `format_exc_info` renders the whole `__cause__` chain, so every `BrokerError` /
    `RequestNotSent` / `OrderRejected` / `OrderNotFound` / `UnknownOrderStatus` that
    `alpaca_http.py` or `alpaca.py` raises uses `from None`.
  - The safe metadata is carried explicitly instead: the cause's type name in the message and
    as the `alpaca.http` `error` field, plus the request id.
  - Text that came from Alpaca (`message`) passes through
    `sanitize(text: object, secrets: Sequence[str]) -> str` in `alpaca_http.py` before it goes
    into a log field or an exception message. It is `str(text)` with every occurrence of the
    key id and the secret replaced by `[redacted]`, control characters removed, and the result
    capped at 200 characters plus `...`. A non-str `message` is converted with `str()` first.
  - **Every other response-derived value** that reaches a log field or an exception message goes
    through `AlpacaHttp.diag(value: object, pattern: re.Pattern[str]) -> str` (Codex r2).
    - The value is shown only if `str(value)` fully matches the pattern for its kind; otherwise
      it becomes `<withheld: N chars>`. The key id and secret are then redacted as in
      `sanitize`. The pattern alone is not enough: the key id is uppercase letters and digits
      and would pass the request-id pattern.
    - Patterns, as module constants in `alpaca_http.py`:
      - `STATUS_RE = [a-z_]{1,32}`: Alpaca order statuses;
      - `REQUEST_ID_RE = [A-Za-z0-9-]{1,64}`: `X-Request-ID`, applied in `_send` before the
        id enters `HttpResult`, so every later use is already clean;
      - `FIELD_RE = [A-Za-z0-9/._:-]{1,64}`: ack fields (ids, symbols, sides, types, TIF,
        decimals).
    - Comparisons and status mapping always use the raw values. Only diagnostics are
      filtered.
- Decimals go out as `format(d, "f")`. Numbers in responses must be JSON strings; a JSON number
  where a decimal is expected raises `BrokerError` (Review Focus 1).
- Never branch on Alpaca's body `code` (spec 3.3).
- `orders_enabled` stays `False` everywhere outside tests. `connect()` has no parameter that can
  set it (spec 3.8).

### Builder calls made here (spec section 5)

- Module `broker/alpaca_http.py`; class `AlpacaHttp`; result type `HttpResult`.
- Pool sizes: read `httpx.Limits(max_connections=4, max_keepalive_connections=4)`, write
  `httpx.Limits(max_connections=4, max_keepalive_connections=0)`. Pool timeout 2 s.
- Cookies: pass the **raw** `http.cookiejar.CookieJar(policy=DefaultCookiePolicy(allowed_domains=[]))`
  as `cookies=`. An empty allow list accepts no cookie. Wrapping it in `httpx.Cookies` copies it
  into a fresh jar without the policy (review I3, reproduced). A test pins this on both
  clients.
- **Closing network backend** (owner decision D22 (9), review C1). httpcore's anyio `start_tls`
  closes the stream only on `Exception`, so a cancel during the TLS handshake leaves an open
  socket that nothing closes. `alpaca_http.py` wraps `httpcore.AnyIOBackend`: its stream's
  `start_tls` closes the inner stream on `BaseException` and re-raises. httpx 0.28 has no public
  hook for a backend, so the transport sets `transport._pool._network_backend` (private;
  pinned by the loopback test and re-checked on upgrade). This is not the DNS-isolating backend
  spec section 1 rejected: it resolves nothing and only adds the missing close.
- `asset_rules` cache TTL 300 s, on an injected monotonic clock.
- The startup broker check gets one overall `asyncio.timeout(BROKER_CHECK_S)` with
  `BROKER_CHECK_S = 10.0` in `core/runtime.py`, the engine's `io_timeout_s` default. The runtime
  does not configure `io_timeout_s`, so there is no config value to reuse.
- `aclose()` joins the `Broker` protocol. SimBroker and the runtime's test fakes get it.
- Log level of `alpaca.http` (owner, 2026-11-22): `info` for every 2xx, `warning` for any non-2xx
  status or transport error. The default `info` level would drop `debug`, and successful reads
  would then log no request id (review M5). Cost: about 2-4 lines per minute steady state, plus
  about 30 per minute per unsettled order.
- Order ids are checked against `[A-Za-z0-9-]+` before they go into a path (in `get_order`,
  `cancel_order` and the ack check). Anything else raises `BrokerError` before any I/O: httpx
  normalises `/orders/../account`, and `DELETE /v2/orders` is Alpaca's cancel-all (review M6).
- Every `BrokerError` raised from a response carries the request id: `UnknownOrderStatus`,
  parse errors and `OrderNotFound` included (spec 3.1, review M4; `OrderNotFound` owner
  decision after the dry run).
- `cancel_order`: any 2xx returns (the probes saw 204). Spec 3.3 lists only 204 and "other
  non-2xx".
- `get_order` also checks that the response's `id` equals the requested id; a different id is
  malformed data and raises `BrokerError` (PLAN 1.6a: malformed broker data raises).
- Response symbols go through `_slash_symbol` before any comparison or view (spec 3.4).
- SimBroker: a full-holding sell (qty == `qty_available`) keeps skipping the increment check
  (spec 3.9). `stream_fills`, `redeliver` and the fill queue go (no ledger test uses
  `redeliver`). Tests that read the stream use `order_fills(order_id)` or the new test hook
  `SimBroker.fills()`.
- Loopback mechanics (test 9): an asyncio TCP server that never answers the TLS ClientHello
  holds the connection in setup deterministically. It also yields a real `ConnectTimeout` with
  a short connect timeout. A closed port yields a real `ConnectError`.

### Request budget (spec 3.1, Fable I5)

Alpaca allows 200 trading-API requests per minute per account. With the `asset_rules` cache:

- **Steady state** (no unsettled order, no flatten): the 60 s snapshot reads `get_account` and
  `get_positions`, and asset rules about once per symbol every 5 min. About **2-4 per minute**.
- **Startup until admission opens:** `admission.ready` reads `get_positions` once per 1 s tick
  (`broker_holdings`). At most **60 per minute**, only until it opens.
- **Peak (corrected 2026-11-23, Codex P2-2):** a pending flatten makes every 1 s Position
  Manager step read `get_positions` **twice**: `_control_phase` → `_sell_external` and
  `_flat_check`, each through `sellable_positions()`; the cache removes neither. Counted:
  - flatten position reads: **120/min**;
  - 3 unsettled orders read every 2 s (`get_order`): **90/min**;
  - **210/min** together, over the 200 limit before anything else;
  - on top: each exit attempt's own `get_positions` and its submit, entry cancels
    (`cancel_open_entries`, once per step while a cancel is due), and resolver lookups.

  Without the cache every sync read and position read would also fetch asset rules.
  `order_fills` (1.6b) adds a read only when `filled_qty` grew.
- **Consequence.** 1.6a sends no writes, so nothing reaches this peak yet. 1.6d may enable
  writes only after a measured run shows headroom. This is recorded as a 1.6d precondition on
  the PLAN 1.6 row:
  - the scenario: a pending flatten, three unsettled orders and a failing exit;
  - the measurement: the request rate over a full minute;
  - the threshold: at most **80 % of the limit (160/min)**;
  - if it is higher: reduce the reads first, for example one `get_positions` per Position
    Manager step shared by `_sell_external` and `_flat_check`, or slower status polls while a
    flatten is pending.

  A 429 is `unknown` for a submit and a failed read for everything else. Running into the limit
  would crowd out reconciliation and protective exits.

### Pre-implementation verification (spec 3.2, owner condition; done in the dry run)

The Fable plan review already verified these against httpcore 1.0.9, httpx 0.28.1 and
anyio 4.16.0. The dry run confirms them against the versions `uv add` actually resolves, then
records them (file, function, versions) in `docs/alpaca-notes.md` (Task 8):

1. `httpcore/_async/connection.py` `AsyncHTTPConnection.handle_async_request`: `_connect()`
   (`connect_tcp`, then `start_tls`) returns before the request reaches the HTTP/1.1
   connection. `ConnectError` / `ConnectTimeout` are produced only by the exception maps in
   `httpcore/_backends/anyio.py` (`connect_tcp`, `connect_unix_socket`, `start_tls`); there is
   no other raise site outside the proxy modules.
2. `httpcore/_async/http11.py` `handle_async_request`: any exception except `WriteError`,
   `CancelledError` included, closes the connection in the `except BaseException` path
   (`_response_closed` under `AsyncShieldCancellation`). `WriteError` is suppressed and the
   response is read. That outcome is a read error or a status, never a connect error.
3. `httpcore/_async/connection_pool.py` and `_synchronization.py`: `PoolTimeout` is raised only
   around the wait for a connection. The retry loop re-queues only on
   `ConnectionNotAvailable`, which `http11.py` raises before any write.
4. `httpx/_transports/default.py` maps `httpcore.ConnectError` / `ConnectTimeout` /
   `PoolTimeout` to the `httpx` exceptions of the same name. None of the three has a subclass,
   and httpx raises them nowhere else.
5. (C1, the reason for the closing backend) `httpcore/_backends/anyio.py` `start_tls` closes
   the stream only on `Exception`, not on cancellation.

The argument holds only under these conditions, which the transport must keep and the notes
must state:
- no proxy (the proxy modules call `start_tls` after written bytes; `trust_env=False` and no
  `proxy=` keep them out);
- HTTP/1.1 only (`http2` off, the default);
- `retries=0`;
- the anyio backend.

**If facts 1-4 or a condition do not hold for the resolved versions, stop and report to the
owner before Task 4.** The `RequestNotSent` mapping would be unsafe, and that changes the
spec. Pin and record anyio too (review M10).

## Review Focus

These are the inputs most likely to break a real run that no spec acceptance test covers. Each
one has a test in the task named in brackets.

1. **A decimal arrives as a JSON number** (`"filled_qty": 0.5`, not `"0.5"`): `Decimal(0.5)`
   would silently become the binary expansion. Expect `BrokerError`. (Task 5,
   `test_a_json_number_where_a_decimal_belongs_is_a_broker_error`)
2. **A 2xx with a non-JSON body** (a proxy or maintenance HTML page): expect `BrokerError` from
   reads, and a submit row `unknown`, not a crash. (Task 5,
   `test_a_2xx_html_body_is_a_broker_error`)
3. **Timestamps with nanoseconds and `Z`** (as observed: `2026-11-12T10:26:03.148318561Z`)
   parse to an aware datetime, truncated to microseconds. A naive timestamp is malformed and
   raises `BrokerError`. (Task 5, `test_nanosecond_z_timestamps_parse_and_naive_ones_fail`)
4. **The ack echoes `BTCUSD` instead of `BTC/USD`**: it is normalised, so it is not a mismatch.
   An unmappable symbol raises `BrokerError`. (Task 5, `test_ack_symbol_is_normalised`)
5. **The submit ack carries a status outside the 3.4 table** (`held`): `UnknownOrderStatus`
   (a `BrokerError`), so the row is `unknown` and the resolver takes over. An IOC ack that is
   already `canceled` maps to `canceled`. (Task 5, `test_ack_with_unlisted_status_is_unknown`)

---

### Task 1: Protocol cleanup, `aclose`, SimBroker cancel contract

Spec 3.7 (protocol part), 3.9, acceptance 13 and 14. No transport yet.

**Files:**
- Modify: `src/autotrader/broker/base.py` (`Broker` protocol)
- Modify: `src/autotrader/broker/sim.py`
- Modify: `src/autotrader/broker/alpaca.py` (drop the three stubs; add a no-op `aclose`, which
  Task 5 replaces)
- Create: `tests/sim_helpers.py`
- Modify: `tests/unit/broker/test_sim.py`, `tests/unit/broker/test_alpaca.py`,
  `tests/unit/risk/test_engine.py`, `tests/unit/core/test_ledger.py`,
  `tests/unit/core/test_runtime.py` (`FakeBroker.aclose`), and any other test mypy flags

**Interfaces:**
- Produces: `Broker` without `close_position`, `close_all`, `stream_fills`; with
  `async def aclose(self) -> None`.
- Produces: `SimBroker.fills() -> list[Fill]` (test hook: every emitted fill, in emission
  order) and `SimBroker.take_fills() -> list[Fill]` (the fills emitted since the previous
  `take_fills()` call).
- Produces: `tests/sim_helpers.py`:
  `async def sell_free(sim: SimBroker, symbol: str) -> OrderAck`. It submits a market sell of
  the position's `qty_available` (what `exit()` does since D20). With no position or a free qty
  of 0 it raises `OrderRejected`, as `close_position` did.
- Produces: SimBroker `cancel_order` contract: canceled returns, filled/rejected raises
  `BrokerError` (not `OrderRejected`), unknown id raises `OrderNotFound`. An open order is
  canceled as today.

- [ ] **Step 1: Write the failing tests** in `tests/unit/broker/test_sim.py`

```python
async def test_cancel_follows_the_alpaca_contract() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    resting = await sim.submit_order(  # gtc limit far below the market: rests
        BrokerOrder(symbol=BTC, side="buy", qty=Decimal("1"), type="limit", limit_price=Decimal("50"))
    )
    await sim.cancel_order(resting.order_id)
    await sim.cancel_order(resting.order_id)  # canceled again: returns (Alpaca 204)
    filled = await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    with pytest.raises(BrokerError) as e:
        await sim.cancel_order(filled.order_id)
    assert not isinstance(e.value, OrderRejected)  # 422, not a rejection
    with pytest.raises(OrderNotFound):
        await sim.cancel_order("sim-999")

def test_removed_methods_are_gone() -> None:
    for name in ("close_position", "close_all", "stream_fills", "redeliver"):
        assert not hasattr(SimBroker, name)
    assert not {"close_position", "close_all", "stream_fills"} & set(dir(Broker))

```

`test_fills_carry_stable_unique_ids_and_fee_asset` (today line 360) becomes the `fills()` test.
It reads `sim.fills()` instead of the stream, with the same assertions. No duplicate test is
added (review M1).

- [ ] **Step 2: Run them to see them fail**

Run: `uv run pytest tests/unit/broker/test_sim.py -k "alpaca_contract or removed_methods or stable_unique_ids" -q`
Expected: 3 failed.

- [ ] **Step 3: Implement**
  - `base.py`: remove the three protocol methods and the now-unused `AsyncIterator` import;
    add `aclose`.
  - `sim.py`: remove `close_position`, `close_all`, `stream_fills`, `redeliver`, the `_fills`
    queue and its `put_nowait`. Also drop the `structlog` import and `log`: their only user
    was `close_all` (dry run). Keep `_fill_log`; `fills()` returns its values.
    `cancel_order` follows the new contract. Add `async def aclose(self) -> None` (no-op).
    Update the module docstring: the `redeliver` sentence goes; the `close_position` bullet
    becomes "a `submit_order` sell of the full free qty"; one line on the cancel contract.
    `_Order.full_close`'s comment no longer names `close_position`.
  - `alpaca.py`: delete the three `_not_yet` stubs; add `async def aclose(self) -> None: return`.
  - Tests: every `close_position` call becomes `sell_free` (test_engine 1271, 1283, 2096;
    test_sim; test_ledger `place(..., "close")`). `SellSpy`'s `close_position` override goes;
    if a test asserted `close_position` was never called, assert instead that every submitted
    sell is a market sell of the free qty. Delete the sim tests that existed only for the
    removed methods (`test_close_position_and_close_all`,
    `test_close_position_without_position_raises`,
    `test_close_all_continues_past_a_rejected_symbol`,
    `test_redeliver_repeats_the_identical_fill`). Keep their full-close and dust cases by
    rewriting them on `sell_free`. Stream-reading tests use `sim.fills()`, the hypothesis
    invariant test included. `drain()` keeps its consuming meaning (tests at 661, 674 and
    830-835 assert `drain(sim) == []` after an earlier drain). It becomes
    `return sim.take_fills()`, a second test hook that returns the fills emitted since its last
    call (a cursor index into `_fill_log` kept by the sim) (review M1). Tests that expect `OrderRejected` from cancelling a
    final order expect `BrokerError`. In `test_alpaca.py`, drop the removed names from the two
    "not implemented" tests; the second one used `close_position` as its write call, so it now
    uses a lazy `submit_order` call (dry run). Add `async def aclose(self) -> None` to `test_runtime.FakeBroker`.
- [ ] **Step 4: Full check** (Global Constraints order). Expected: all green, `risk/` 100 %.
- [ ] **Step 5: Commit**

```bash
git add src/autotrader/broker/base.py src/autotrader/broker/sim.py src/autotrader/broker/alpaca.py tests/
git commit -m "refactor: drop close_position, close_all and stream_fills from the Broker protocol (WP 1.6a)"
```

---

### Task 2: `BrokerOrderView` gains type, limit price, TIF; `mismatches` compares them

Spec 3.6, acceptance 11.

**Files:**
- Modify: `src/autotrader/broker/base.py` (`BrokerOrderView`)
- Modify: `src/autotrader/risk/resolve.py`
- Modify: `src/autotrader/risk/engine.py:613` (`_decide` call), `src/autotrader/core/repair.py:131`
- Modify: `src/autotrader/broker/sim.py` (`get_order_by_client_id`)
- Test: `tests/unit/risk/test_resolve.py`, `tests/unit/risk/test_resolver.py`,
  `tests/unit/core/test_repair.py`, `tests/unit/broker/test_sim.py`

**Interfaces:**
- Produces: `BrokerOrderView.type: OrderType`, `.limit_price: Dec | None`,
  `.time_in_force: TimeInForce` (required fields, no defaults).
- Produces: `expected_tif(order_type: str) -> TimeInForce | None` in `risk/resolve.py`:
  `"limit"` gives `"ioc"`, `"market"` gives `"gtc"`, anything else gives `None`.
- Produces: `mismatches(view, *, symbol, side, qty, order_type: str, limit_price: Decimal | None,
  submitted_at, broker_order_id) -> tuple[str, ...]` with three new lines:
  `type {view.type} != {order_type}`, `limit_price {view.limit_price} != {limit_price}`
  (Decimal equality; `None` equals only `None`), and
  `time_in_force {view.time_in_force} != {expected}` (with `expected=None` the line reads
  `... != none for type {order_type}`).
- Callers pass `order_type=row.type, limit_price=row.limit_price`.

- [ ] **Step 1: Write the failing tests** in `tests/unit/risk/test_resolve.py` (extend its `view()`
  base with `type="limit", limit_price=D("100"), time_in_force="ioc"` and its `check()` with
  `order_type="limit", limit_price=D("100")`)

```python
def test_type_limit_price_and_tif_mismatches_are_reported() -> None:
    assert check(view(type="market", limit_price=None, time_in_force="gtc")) == (
        "type market != limit",
        "limit_price None != 100",
        "time_in_force gtc != ioc",
    )
    assert check(view(limit_price=D("100.0"))) == ()  # Decimal equality, not text
    assert check(view(time_in_force="gtc")) == ("time_in_force gtc != ioc",)

def test_market_row_expects_gtc_and_no_limit() -> None:
    v = view(type="market", limit_price=None, time_in_force="gtc")
    assert check(v, order_type="market", limit_price=None) == ()

def test_unknown_row_type_is_a_mismatch() -> None:
    assert expected_tif("stop") is None
    assert "time_in_force ioc != none for type stop" in check(view(), order_type="stop")
```

Plus one engine resolver test in `test_resolver.py`,
`test_client_id_hit_differing_only_in_limit_price_is_a_mismatch`: a type (a) row whose broker
view differs only in `limit_price` becomes a `client_id_mismatch` incident. Plus one repair test
in `test_repair.py`, `test_link_refuses_a_limit_price_mismatch`: `prepare_link` refuses with
"limit_price" in the reason. Setup for both (dry run):
- `tr.insert_row` gains `type_` and `limit_price` keyword arguments (today it hard-codes a
  market row).
- The sim order is an IOC limit buy, qty 0.01 at 100; the row says 101. Prices of 10/11 fail
  the sim's `min_notional` of 1.

- [ ] **Step 2: Run them to see them fail**

Run: `uv run pytest tests/unit/risk/test_resolve.py -q`
Expected: FAIL (collection ImportError on `expected_tif`) (dry run).

- [ ] **Step 3: Implement** the fields, `expected_tif`, the `mismatches` lines (after `qty`,
  before `created_at`), both callers, and SimBroker's view (`type`, `limit_price`,
  `time_in_force` from the stored `BrokerOrder`). Fix every test that builds a
  `BrokerOrderView`. pytest finds them, mypy does not:
  `test_sim.py::test_get_order_by_client_id_returns_view_or_none` fails only at runtime (dry run). Update the `mismatches` docstring and the
  `core/repair.py` module docstring ("matched symbol, side, qty, type, limit price, time in
  force and the `created_at` bound").
- [ ] **Step 4: Full check.** Expected: green, `risk/` 100 %.
- [ ] **Step 5: Commit**

```bash
git add src/autotrader/broker/base.py src/autotrader/risk/resolve.py src/autotrader/risk/engine.py src/autotrader/core/repair.py src/autotrader/broker/sim.py tests/
git commit -m "feat: compare order type, limit price and time in force in the resolver match check (WP 1.6a)"
```

---

### Task 3: `RequestNotSent` and `UnknownOrderStatus` through engine, resolver and repair

Spec 3.2 (engine mapping), 3.4 (unknown status path, `unknown_status` repair), acceptance 2
(engine part, with SimBroker stubs), 3 and 5 (engine/repair part). The exceptions are defined
here so the engine is complete before the adapter raises them.

**Files:**
- Modify: `src/autotrader/broker/base.py`
- Modify: `src/autotrader/risk/resolve.py` (`IncidentReason`)
- Modify: `src/autotrader/risk/engine.py` (`_submit`, `_read`, `_decide`, module docstring)
- Modify: `src/autotrader/core/repair.py` (`LINKABLE_INCIDENTS`, `prepare_link`)
- Test: `tests/unit/risk/test_engine.py`, `tests/unit/risk/test_resolver.py`,
  `tests/unit/core/test_position_manager.py`, `tests/unit/core/test_repair.py`

**Interfaces:**
- Produces in `broker/base.py`:
  - `class RequestNotSent(BrokerError)`, docstring: "The request failed before any byte of it
    was written (connect, DNS, TLS, pool wait); no order can exist. Submit records
    `not_submitted` (D22.5)."
  - `class UnknownOrderStatus(BrokerError)` with
    `__init__(self, status: str, request_id: str | None = None)`. It stores `self.status` and
    has the message `f"unmapped alpaca order status {status!r} (request {request_id})"`
    (review M4).
- Produces: `IncidentReason` gains `"unknown_status"`.
- Produces: `LINKABLE_INCIDENTS` gains `"unknown_status"`. For a row whose incident is
  `unknown_status`, `prepare_link` refuses unless `view.status in ("filled", "canceled",
  "rejected")`, with `RepairRefused(f"broker order is not final: {view.status}")`, checked after
  the lookup and before `mismatches`.
- Engine behaviour:
  - `_submit`: `except RequestNotSent as e` before the generic handler (after
    `OrderMethodsUnavailable`) logs `risk.order_not_submitted` with
    `reason=f"request not sent: {type(e.__cause__ or e).__name__}"` and records
    `not_submitted`.
  - `_read`: `except UnknownOrderStatus as e` next to `OrderNotFound`: log
    `risk.order_status_unknown` (warning, `order_row`, `status=e.status`), append to `failed`
    and to `candidates` as type `"b"`.
  - `_decide`: `except UnknownOrderStatus` before the generic `except Exception`: log
    `risk.order_status_unknown` and set `outcome = "unknown_status"`.
  - **The direct read's evidence is kept (Codex P2-3, widened by Codex r2).**
    - The engine keeps `self._status_unknown: set[int]`, an in-memory set like `_last_outcome`.
      `_read` adds a row when its `get_order` raises `UnknownOrderStatus`. The row is removed
      only when a successful direct read clears it (the `clear_clock` path), or when the row
      leaves resolution (`_resolve` prunes it to `live` rows, as it does `_last_outcome`).
    - A direct read that later raises `OrderNotFound` or fails does not clear the evidence.
      `_resolve` hands `status_unknown=row.id in self._status_unknown` to `_decide`.
    - **Restart limitation (owner, accepted):** the set is in memory, so a restart inside the
      resolution window forgets the evidence. That run behaves as before this change (an
      inconclusive lookup can end as `lookup_failed`). The runbook says so in the
      `unknown_status` section (Task 8).
    - In `_decide`, for such a row every inconclusive outcome becomes `"unknown_status"`, both
      this pass's and the remembered one: `lookup_failed`, `not_found`,
      `lookup_inconsistent`, or not looked up because the budget was spent.
    - Only `client_id_mismatch`, which is returned at once, still wins.
    - So a 429 or timeout on the client-id lookup at the bound cannot turn the incident into
      `lookup_failed`. That reason is linkable without the finality check.

- [ ] **Step 1: Write the failing tests**

`test_engine.py` (SimBroker subclasses in the style of `SellSpy`):

```python
class UnsentSubmit(SimBroker):
    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        raise RequestNotSent("alpaca POST /orders not sent: ConnectError")

async def test_an_unsent_entry_is_not_submitted(...) -> None:
    # check() an approved intent against UnsentSubmit
    result = await engine.check(intent(cycle_id))
    assert result.status == "not_submitted"
    (row,) = await orders(sessions)
    assert (row.status, row.broker_order_id) == ("not_submitted", None)

async def test_an_unsent_exit_is_not_submitted(...) -> None: ...  # same through exit()
    # Seed the BTC holding with SimBroker.submit_order(broker, <market buy>), which bypasses
    # the raising override (dry run).

@pytest.mark.parametrize("cls", [WrittenThenFailed, HangingSubmit])
async def test_a_failure_after_the_write_stays_unknown(cls: type[SimBroker], ...) -> None:
    # WrittenThenFailed: submit raises BrokerError("... WriteError"); HangingSubmit (existing):
    # submit outlives io_timeout_s=0.1 on a Mono() clock. One fresh journal per case. Each row
    # ends "unknown" (dry run: a second check() on the same journal sees the first unknown row)
```

`test_resolver.py` (sketch; use the file's `Rig` and its `sync_at`, which applies each result
through the ledger, so `start_clock` is persisted (review M9)):

```python
class HeldStatus(SimBroker):  # every read of any order meets Alpaca status "held"
    async def get_order(self, order_id: str) -> OrderStatus:
        raise UnknownOrderStatus("held", "rid-1")
    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrderView | None:
        raise UnknownOrderStatus("held", "rid-2")

async def test_an_unknown_status_read_makes_a_type_b_candidate_then_an_incident(...) -> None:
    # a submitted entry on HeldStatus (r.entry()), so the row has a broker id
    first = await r.sync_at(0)
    assert row_id in first.failed
    assert actions(first) == [(row_id, "start_clock", None)]
    later = await r.sync_at(80)  # order_resolution_timeout_s after the clock started
    assert (row_id, "incident", "unknown_status") in actions(later)
    assert (await order(r.sessions, row_id)).incident == "unknown_status"
```

```python
class HeldButLookupFails(HeldStatus):  # Codex P2-3: the lookup is inconclusive at the bound
    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrderView | None:
        raise BrokerError("alpaca GET /orders:by_client_order_id returned 429 (request rid-3)")

async def test_a_failed_lookup_cannot_downgrade_an_unknown_status(...) -> None:
    # same flow as above on HeldButLookupFails
    await r.sync_at(0)
    later = await r.sync_at(80)
    assert (row_id, "incident", "unknown_status") in actions(later)  # not "lookup_failed"


class HeldThenGone(HeldButLookupFails):  # Codex r2: the evidence outlives the pass
    reads = 0
    async def get_order(self, order_id: str) -> OrderStatus:
        self.reads += 1
        if self.reads == 1:
            raise UnknownOrderStatus("held", "rid-1")
        raise OrderNotFound(f"{order_id} (request rid-4)")

async def test_unknown_status_evidence_lasts_the_resolution_window(...) -> None:
    # pass 1: direct read "held"; pass 2 (at the bound): direct read 404, lookup 429
    await r.sync_at(0)
    later = await r.sync_at(80)
    assert (row_id, "incident", "unknown_status") in actions(later)

async def test_a_successful_read_clears_the_unknown_status_evidence(...) -> None:
    # pass 1 "held"; pass 2 the direct read succeeds (row readable again, clear_clock);
    # pass 3 a fresh 404 plus a 429 lookup at that window's bound -> "lookup_failed", not
    # "unknown_status" (the evidence was cleared)
```

`actions()` returns `(row, action, reason)` triples. Check its exact tuple shape in the file and
adapt only that. The new `status_unknown` branch in `_decide` is covered by this test (`risk/`
100 %).

`test_position_manager.py` (Harness, as in `test_failed_exits_back_off_and_alert_after_three`):

```python
class UnsentOnce(SimBroker):
    sent = False
    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        if order.side == "sell" and not self.sent:
            self.sent = True
            raise RequestNotSent("stub")
        return await super().submit_order(order)

async def test_an_unsent_exit_is_retried_after_its_backoff(sessions, bus) -> None:
    h = Harness(sessions, bus, sim=make_sim(cls=UnsentOnce))
    await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()
    assert [s.status for s in await h.sells()] == ["not_submitted"]
    await h.pm.step()  # within the 1 s backoff: nothing
    assert len(await h.sells()) == 1
    h.clock.advance(1)
    await h.pm.step()
    assert [s.status for s in await h.sells()] == ["not_submitted", "new"]
```

`test_repair.py` sketch:
- The row is inserted with
  `tr.insert_row(r.sessions, cid=cid, status="new", broker_order_id=oid)` for an order
  submitted on `r.sim`.
- It is then set to the incident directly:
  `update(m.Order).values(incident="unknown_status", incident_at=NOW)`. Driving 80 s of sync
  passes is unnecessary here (dry run).
- The lookups are inline async functions. `sim` below is `r.sim`.

```python
async def test_link_of_an_unknown_status_incident_needs_a_final_broker_order(r: Rig) -> None:
    async def held(cid: str) -> BrokerOrderView | None:
        raise UnknownOrderStatus("held", "rid-1")
    with pytest.raises(RepairRefused, match="broker lookup failed: UnknownOrderStatus"):
        await prepare_link(r.sessions, row, broker_id, held)
    # the sim's order is open ("new"): refused
    with pytest.raises(RepairRefused, match="broker order is not final: new"):
        await prepare_link(r.sessions, row, broker_id, lookup_of(sim))
    await sim.cancel_order(broker_id)
    plan = await prepare_link(r.sessions, row, broker_id, lookup_of(sim))
    assert plan.after["incident"] is None
```

- [ ] **Step 2: Run them to see them fail**

Run: `uv run pytest tests/unit/risk tests/unit/core/test_position_manager.py tests/unit/core/test_repair.py -q -k "unsent or after_the_write or unknown_status"`
Expected: FAIL (ImportError on `RequestNotSent`).

- [ ] **Step 3: Implement** as listed under Interfaces. Engine module docstring: type (b) now
  also covers "`get_order` raised `UnknownOrderStatus`"; `not_submitted` also covers "a request
  the transport proved unsent (`RequestNotSent`)". `repair.py` docstring: `unknown_status` is
  linkable only once the broker order is final. `OrderMethodsUnavailable` docstring in
  `base.py`: "the adapter refuses order writes (Alpaca until WP 1.6d) or cannot serve the call
  yet (`order_fills` until WP 1.6b)".
- [ ] **Step 4: Full check.** Expected: green, `risk/` 100 % (both new `except` branches are
  covered by the tests above).
- [ ] **Step 5: Commit**

```bash
git add src/autotrader/broker/base.py src/autotrader/risk/ src/autotrader/core/repair.py tests/
git commit -m "feat: record provably unsent requests not_submitted and give unknown order statuses a resolver path (WP 1.6a)"
```

---

### Task 4: The HTTP transport `broker/alpaca_http.py`

Spec 3.1, 3.2 (invariants, mapping table), acceptance 6, 7, 8, 9 (transport part). Run the
Pre-implementation verification first.

**Files:**
- Modify: `pyproject.toml`, `uv.lock`
  (`uv add "httpx>=0.28.1,<0.29" "httpcore>=1.0.9,<1.1" "anyio>=4.16,<5"`). httpcore and anyio
  are listed directly only to pin what the safety facts were verified against (owner, after
  the dry run).
- Create: `src/autotrader/broker/alpaca_http.py`
- Test: `tests/unit/broker/test_alpaca_http.py`, `tests/unit/broker/loopback.py` (test servers)

**Interfaces:**
- Produces:

```python
TIMEOUT = httpx.Timeout(connect=3.0, write=5.0, read=5.0, pool=2.0)
READ_LIMITS = httpx.Limits(max_connections=4, max_keepalive_connections=4)
WRITE_LIMITS = httpx.Limits(max_connections=4, max_keepalive_connections=0)
PAPER_URL = "https://paper-api.alpaca.markets/v2"
LIVE_URL = "https://api.alpaca.markets/v2"

@dataclass(frozen=True)
class HttpResult:
    status: int
    body: Any          # parsed JSON; None for an empty body or one that is not JSON
    parsed: bool       # True iff the body was non-empty, valid JSON
    request_id: str | None   # X-Request-ID

class AlpacaHttp:
    def __init__(self, *, base_url: str, key_id: str, secret_key: str,
                 transport: httpx.AsyncBaseTransport | None = None,
                 timeout: httpx.Timeout = TIMEOUT) -> None: ...
    async def get(self, template: str, path: str, *, params: Mapping[str, str] | None = None,
                  order_id: str | None = None) -> HttpResult: ...
    async def post(self, template: str, path: str, payload: Mapping[str, str]) -> HttpResult: ...
    async def delete(self, template: str, path: str, *, order_id: str | None = None) -> HttpResult: ...
    async def aclose(self) -> None: ...          # closes both clients
    @property
    def closed(self) -> bool: ...                 # both clients closed

def wire_decimal(value: Decimal) -> str: ...      # format(value, "f")

def make_transport(limits: httpx.Limits) -> httpx.AsyncHTTPTransport: ...
    # AsyncHTTPTransport(limits=limits, retries=0, verify=True, trust_env=False, http2=False),
    # then `transport._pool._network_backend = ClosingBackend()` (review C1, I2)

class ClosingBackend(httpcore.AsyncNetworkBackend): ...   # wraps httpcore.AnyIOBackend
class _ClosingStream(httpcore.AsyncNetworkStream): ...    # delegates read/write/aclose/get_extra_info
```

- `ClosingBackend.connect_tcp(...)` wraps the inner stream in `_ClosingStream`;
  `connect_unix_socket` raises `NotImplementedError`; `sleep` delegates. This is the one
  algorithm the signatures do not fix:

```python
async def start_tls(self, ssl_context, server_hostname=None, timeout=None):  # _ClosingStream
    try:
        return await self._inner.start_tls(ssl_context, server_hostname, timeout)
    except BaseException:  # httpcore closes only on Exception; a cancel leaked the socket
        await self._inner.aclose()
        raise
```

  Fully typed in the real file. The review validated this in a scratch venv: 3 runs green under
  `filterwarnings=error`, for both the `task.cancel()` and the `wait_for` case.
- `transport=None` builds both clients' transports with `make_transport` (`READ_LIMITS` /
  `WRITE_LIMITS`). A given `transport` (tests) serves both clients, so the write client then
  keeps alive whatever that transport keeps alive. The `AlpacaHttp` and `connect()` docstrings
  say: "tests only; one transport serves both clients" (dry run design point 4). Both clients:
  `trust_env=False`, `follow_redirects=False`, `timeout=timeout`, the raw cookie jar.
- `get` uses the read client; `post` and `delete` use the write client.
- Exception mapping (spec 3.2 table, widened by review I7). The message never includes
  `str(e)`, and every raise uses `from None`, so the rendered log has no cause chain (Codex
  P2-1). `CancelledError` (a `BaseException`) passes through.
  - A call on a closed client (`client.is_closed`, checked first, before any I/O) raises
    `RequestNotSent(f"alpaca {method} {template} not sent: client closed")`. A task that
    teardown left behind and that submits after the broker step is provably unsent, not
    `unknown`.
  - `httpx.ConnectError`, `httpx.ConnectTimeout` and `httpx.PoolTimeout` raise
    `RequestNotSent(f"alpaca {method} {template} not sent: {type(e).__name__}")` from None.
  - **Any other `Exception`** raises
    `BrokerError(f"alpaca {method} {template} failed: {type(e).__name__}")` from None. That
    includes other httpx errors, a bare `OSError` (anyio maps only some), and a `RuntimeError`.
- `alpaca.http` log line per call: `method`, `path` (the template), `order_id` (when given),
  `status` or `error` (exception type name), `elapsed_ms` (int, monotonic), `request_id`, and
  for non-2xx `message` (`sanitize(body.get("message"), (key_id, secret_key))` when the body is
  a dict; `AlpacaHttp` exposes `sanitize_message(text) -> str` bound to its own keys for
  `alpaca.py`). Levels: `info` for
  every 2xx, `warning` otherwise.
- HTTP status codes are returned, never raised: the status contract lives in `alpaca.py`.

- [ ] **Step 1: Pre-implementation verification** (section above). Run
  `uv add "httpx>=0.28.1,<0.29" "httpcore>=1.0.9,<1.1" "anyio>=4.16,<5"`, then confirm facts
  1-5 and the conditions in `.venv/lib/python3.12/site-packages/{httpcore,httpx}` for the
  resolved versions. Record the anyio and h11 versions. Keep the findings (file, function,
  versions) for Task 8. Stop if facts 1-4 or a condition fail.

- [ ] **Step 2: Write the failing tests** in `tests/unit/broker/test_alpaca_http.py`

```python
KEY, SECRET = "KEYID-not-a-real-key-7", "SECRET-not-a-real-secret-9"
Handler = (
    Callable[[httpx.Request], httpx.Response]
    | Callable[[httpx.Request], Coroutine[None, None, httpx.Response]]
)


def http(handler: Handler, **kw: Any) -> AlpacaHttp:
    return AlpacaHttp(
        base_url=PAPER_URL, key_id=KEY, secret_key=SECRET,
        transport=httpx.MockTransport(handler), **kw,
    )


async def test_clients_carry_the_specified_settings() -> None:
    h = http(lambda r: httpx.Response(200, json={}))
    for c in (h._read, h._write):  # private access in tests only
        assert c.timeout == httpx.Timeout(connect=3.0, write=5.0, read=5.0, pool=2.0)
        assert c.follow_redirects is False and c.trust_env is False
        assert c.headers["APCA-API-KEY-ID"] == KEY
    await h.aclose()


async def test_production_transports_ignore_env_and_never_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:  # review I2, M7: the settings MockTransport hides
    monkeypatch.setenv("SSL_CERT_FILE", "/nonexistent/ca.pem")  # trust_env would load it
    h = AlpacaHttp(base_url=PAPER_URL, key_id=KEY, secret_key=SECRET)
    for client, keepalive in ((h._read, 4), (h._write, 0)):
        pool = client._transport._pool  # type: ignore[attr-defined]
        assert pool._retries == 0 and pool._http2 is False
        assert pool._max_keepalive_connections == keepalive
        assert pool._ssl_context.verify_mode is ssl.CERT_REQUIRED
        assert pool._ssl_context.check_hostname is True
        assert isinstance(pool._network_backend, ClosingBackend)
    await h.aclose()


async def test_a_set_cookie_is_not_kept() -> None:
    h = http(lambda r: httpx.Response(200, json={}, headers={"set-cookie": "a=b; Path=/"}))
    await h.get("/account", "/account")
    await h.post("/orders", "/orders", {})  # the write client sees a Set-Cookie too
    assert len(h._read.cookies.jar) == 0 and len(h._write.cookies.jar) == 0
    await h.aclose()


@pytest.mark.parametrize("status", [429, 503])
async def test_no_retry_on_status(status: int) -> None:
    seen: list[httpx.Request] = []

    def handler(r: httpx.Request) -> httpx.Response:
        seen.append(r)
        return httpx.Response(status, json={"message": "x"})

    result = await http(handler).post("/orders", "/orders", {"symbol": "BTC/USD"})
    assert (result.status, len(seen)) == (status, 1)


@pytest.mark.parametrize(
    ("exc", "raised"),
    [
        (httpx.ConnectError, RequestNotSent),
        (httpx.ConnectTimeout, RequestNotSent),
        (httpx.PoolTimeout, RequestNotSent),
        (httpx.WriteError, BrokerError),
        (httpx.WriteTimeout, BrokerError),
        (httpx.ReadError, BrokerError),
        (httpx.ReadTimeout, BrokerError),
        (httpx.RemoteProtocolError, BrokerError),
    ],
)
async def test_transport_errors_map_by_whether_bytes_may_have_left(
    exc: type[httpx.TransportError], raised: type[BrokerError]
) -> None:
    seen: list[httpx.Request] = []

    def handler(r: httpx.Request) -> httpx.Response:
        seen.append(r)
        raise exc("boom", request=r)

    with pytest.raises(raised) as e:
        await http(handler).post("/orders", "/orders", {})
    assert type(e.value) is raised and len(seen) == 1  # exact class, no retry
    assert KEY not in str(e.value) and SECRET not in str(e.value)


async def test_a_rendered_exception_log_carries_no_cause_text_or_secret() -> None:  # Codex P2-1
    def handler(r: httpx.Request) -> httpx.Response:
        raise httpx.ReadError(f"MARKER-cause {SECRET}", request=r)

    buf = io.StringIO()
    configure_logging(stream=buf)  # production chain; tests/conftest.py restores the config
    try:
        await http(handler).post("/orders", "/orders", {})
    except BrokerError:
        structlog.get_logger("t").exception("risk.order_unknown")
    rendered = buf.getvalue()
    assert "ReadError" in rendered and "risk.order_unknown" in rendered
    assert "MARKER-cause" not in rendered and SECRET not in rendered and KEY not in rendered


async def test_alpaca_message_text_is_sanitised() -> None:  # Codex P2-1
    echoed = f"bad key {KEY} / {SECRET}\n" + "x" * 500
    h = http(lambda r: httpx.Response(403, json={"message": echoed}))
    with capture_logs() as logs:
        await h.post("/orders", "/orders", {})
    (line,) = [x for x in logs if x["event"] == "alpaca.http"]
    assert KEY not in line["message"] and SECRET not in line["message"]
    assert "[redacted]" in line["message"] and "\n" not in line["message"]
    assert len(line["message"]) <= 203


async def test_response_derived_diagnostics_are_filtered() -> None:  # Codex r2
    h = http(lambda r: httpx.Response(500, json={}, headers={"X-Request-ID": f"rid {SECRET}"}))
    with capture_logs() as logs:
        result = await h.get("/account", "/account")
    assert result.request_id == "<withheld: " + str(len(f"rid {SECRET}")) + " chars>"
    assert SECRET not in repr(logs)
    assert h.diag("held", STATUS_RE) == "held"
    assert h.diag(f"held{SECRET}", STATUS_RE).startswith("<withheld")
    assert h.diag(KEY, REQUEST_ID_RE) == "[redacted]"  # the pattern alone would let it through


@pytest.mark.parametrize("error", [OSError("reset"), RuntimeError("odd")])
async def test_any_other_exception_is_a_broker_error(error: Exception) -> None:  # review I7
    def handler(r: httpx.Request) -> httpx.Response:
        raise error

    with pytest.raises(BrokerError) as e:
        await http(handler).get("/account", "/account")
    assert type(e.value) is BrokerError


async def test_a_closed_client_is_request_not_sent() -> None:  # review I7
    seen: list[httpx.Request] = []

    def handler(r: httpx.Request) -> httpx.Response:
        seen.append(r)
        return httpx.Response(200, json={})

    h = http(handler)
    await h.aclose()
    assert h.closed
    with pytest.raises(RequestNotSent, match="client closed"):
        await h.post("/orders", "/orders", {})
    assert seen == []


def test_decimals_go_out_without_exponent() -> None:
    assert wire_decimal(Decimal("1E-7")) == "0.0000001"
    assert wire_decimal(Decimal("73667.30052")) == "73667.30052"


async def test_one_log_line_per_call_without_secrets() -> None:
    responses = iter([
        httpx.Response(403, json={"message": "cost basis"}, headers={"X-Request-ID": "rid-1"}),
        httpx.Response(200, json={}, headers={"X-Request-ID": "rid-2"}),
    ])
    h = http(lambda r: next(responses))
    with capture_logs() as logs:
        await h.post("/orders", "/orders", {})
        await h.get("/orders/{id}", "/orders/o-1", order_id="o-1")
    lines = [line for line in logs if line["event"] == "alpaca.http"]
    assert [
        (x["method"], x["path"], x.get("order_id"), x["status"], x["log_level"], x["request_id"])
        for x in lines
    ] == [
        ("POST", "/orders", None, 403, "warning", "rid-1"),
        ("GET", "/orders/{id}", "o-1", 200, "info", "rid-2"),  # 2xx reads log at info (M5)
    ]
    assert lines[0]["message"] == "cost basis"
    assert KEY not in repr(logs) and SECRET not in repr(logs)


async def test_a_handler_blocked_past_cancellation_records_nothing() -> None:
    # Proves only that AlpacaHttp neither shields nor detaches the request (spec wording);
    # the loopback test below carries the real claim (review M8).
    started, release = asyncio.Event(), asyncio.Event()
    completed: list[httpx.Request] = []

    async def handler(r: httpx.Request) -> httpx.Response:
        started.set()
        await release.wait()
        completed.append(r)
        return httpx.Response(200, json={})

    h = http(handler)
    task = asyncio.create_task(h.post("/orders", "/orders", {}))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
    await asyncio.sleep(0)
    assert completed == []
```

Loopback tests. `tests/unit/broker/loopback.py` provides two servers:
- `SilentTls` records every byte, never answers, sets `hello` on the first bytes and `closed`
  on EOF. It also has `record_types() -> set[int]`, which walks the TLS records.
- `KeepAliveHttp` is a minimal HTTP/1.1 responder. It counts accepted connections and answers
  each request with `200 {}`.
- Both servers close their accepted writers before `Server.wait_closed()`, which waits for open
  connections on Python 3.12. Use `contextlib.suppress` there (ruff SIM105) (dry run).

`free_port()` binds port 0 and closes the socket again. Each server closes its client
transports on `__aexit__`. Every `await` on a server event sits inside `asyncio.timeout(10)`.
That is a hang guard, not a timing assertion: with the closing backend, EOF arrives at once.

```python
@pytest.mark.parametrize("how", ["cancel", "wait_for"])
async def test_a_request_cancelled_in_tls_setup_writes_nothing_and_closes(how: str) -> None:
    async with SilentTls() as srv:
        h = AlpacaHttp(base_url=f"https://127.0.0.1:{srv.port}/v2", key_id=KEY, secret_key=SECRET)
        call = h.post("/orders", "/orders", {"symbol": "BTC/USD"})
        if how == "cancel":
            task = asyncio.create_task(call)
            async with asyncio.timeout(10):
                await srv.hello.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:  # the engine's path: wait_for times the caller out during the handshake
            task = asyncio.create_task(asyncio.wait_for(call, 3600))
            async with asyncio.timeout(10):
                await srv.hello.wait()
            task.cancel()  # stands in for the deadline firing; same cancellation inside wait_for
            with pytest.raises(asyncio.CancelledError):
                await task
        async with asyncio.timeout(10):
            await srv.closed.wait()  # EOF before aclose: the backend closed the socket (C1)
        await h.aclose()
    assert srv.record_types() == {0x16}  # TLS handshake records only, no application data
    assert b"POST" not in srv.received and SECRET.encode() not in srv.received


async def test_a_connect_timeout_and_a_refused_port_are_request_not_sent() -> None:
    async with SilentTls() as srv:
        h = AlpacaHttp(
            base_url=f"https://127.0.0.1:{srv.port}/v2", key_id=KEY, secret_key=SECRET,
            timeout=httpx.Timeout(5.0, connect=0.05),
        )
        with pytest.raises(RequestNotSent, match="ConnectTimeout"):
            await h.post("/orders", "/orders", {})
        await h.aclose()
    port = free_port()
    h = AlpacaHttp(base_url=f"http://127.0.0.1:{port}/v2", key_id=KEY, secret_key=SECRET)
    with pytest.raises(RequestNotSent, match="ConnectError"):
        await h.post("/orders", "/orders", {})
    await h.aclose()


async def test_writes_open_a_fresh_connection_reads_reuse_one() -> None:
    async with KeepAliveHttp() as srv:
        h = AlpacaHttp(base_url=f"http://127.0.0.1:{srv.port}/v2", key_id=KEY, secret_key=SECRET)
        await h.post("/orders", "/orders", {})
        await h.post("/orders", "/orders", {})
        assert srv.connections == 2
        await h.get("/account", "/account")
        await h.get("/account", "/account")
        assert srv.connections == 3
        await h.aclose()
    assert h.closed
```

The review verified the reuse counts (2, then 3) and the TLS-only bytes with httpx 0.28.1 /
httpcore 1.0.9. The `wait_for` case cancels the outer task rather than waiting for a real
deadline: `wait_for` turns its own deadline into exactly this cancellation of the inner call,
and the test then needs no clock.

- [ ] **Step 3: Run them to see them fail**

Run: `uv run pytest tests/unit/broker/test_alpaca_http.py -q`
Expected: FAIL (module missing).

- [ ] **Step 4: Implement `AlpacaHttp`, `make_transport` and `ClosingBackend`** per
  Interfaces. One private `_send(client, method, template, path, **kw) -> HttpResult` does the
  closed check, the timing, the mapping and the log line. Body: `parsed` is True only for a
  non-empty body that `response.json()` decodes; a decode error gives `body=None,
  parsed=False`, never an exception. The private attributes the production-transport test
  reads (`_pool._retries`, `_http2`, `_max_keepalive_connections`, `_ssl_context`,
  `_network_backend`) were checked by the review for httpcore 1.0.9. If one is named
  differently in the resolved version, adapt the test, not the transport.
- [ ] **Step 5: Run the tests**, then the full check. Expected: green. Run the loopback file 5
  times (`for i in 1 2 3 4 5; do uv run pytest tests/unit/broker/test_alpaca_http.py -q || break; done`):
  5 passes.
- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock src/autotrader/broker/alpaca_http.py tests/unit/broker/test_alpaca_http.py tests/unit/broker/loopback.py
git commit -m "feat: add the httpx trading-API transport with fixed timeouts and no retries (WP 1.6a)"
```

---

### Task 5: `AlpacaBroker` on the transport: reads, order methods, contract, switch

Spec 3.3, 3.4, 3.5 (adapter part), 3.7 (`connect` and `aclose`), 3.8, acceptance 1, 4, 5
(mapping part), 10, 12, 15 (adapter part), Review Focus 1-5.

**Files:**
- Modify: `src/autotrader/broker/alpaca.py` (rewrite the transport part; keep `FEE_TIERS`,
  `_slash_symbol`, `_parsing`)
- Modify: `src/autotrader/broker/alpaca_data.py`. It imports `_call` from `alpaca.py`, so
  `_call` moves here unchanged with its `asyncio` and `BrokerError` imports. Market data stays
  on alpaca-py and `to_thread` until 1.6c (dry run defect; spec 3.5 gets a dated correction)
- Rewrite: `tests/unit/broker/test_alpaca.py`
- Create: `tests/unit/broker/alpaca_mock.py` (fixture loader + routing `MockTransport`)
- Modify: `tests/smoke/test_alpaca_paper.py` (fixture closes the broker; read-only order
  lookups, acceptance 16)

**Interfaces:**
- Consumes: `AlpacaHttp`, `HttpResult`, `wire_decimal`, `PAPER_URL`, `LIVE_URL` (Task 4);
  `RequestNotSent`, `UnknownOrderStatus` (Task 3); `BrokerOrderView` fields (Task 2).
- Produces:

```python
ASSET_RULES_TTL_S = 300.0

class AlpacaBroker:
    def __init__(self, http: AlpacaHttp, *, paper: bool, fees: FeeSchedule,
                 orders_enabled: bool = False,
                 monotonic: Callable[[], float] = time.monotonic) -> None: ...
        # raises BrokerError("order writes are enabled for paper accounts only") if
        # orders_enabled and not paper
    @classmethod
    async def connect(cls, settings: Settings, *,
                      transport: httpx.AsyncBaseTransport | None = None) -> "AlpacaBroker": ...
    async def aclose(self) -> None: ...

def order_state(raw: str, filled_qty: Decimal, request_id: str | None = None) -> OrderState: ...
    # 3.4 table; raises UnknownOrderStatus(raw, request_id)
```

- `order_state` stays pure. Every caller in `alpaca.py` catches its `UnknownOrderStatus` and
  re-raises `UnknownOrderStatus(http.diag(e.status, STATUS_RE), rid) from None`, so the status
  the engine logs (`risk.order_status_unknown`, `status=e.status`) is already filtered (Codex
  r2).

- `_parsing(what: str, request_id: str | None)` turns the `TypeError` / `ValueError` /
  `KeyError` / `ArithmeticError` from parsing into `BrokerError` with ` (request <id>)`, and
  appends the id to a nested `BrokerError` (`_slash_symbol`, ack mismatch) whose message lacks
  one (review M4, dry run). `_decimal(raw: object) -> Decimal` and
  `_aware(raw: object) -> datetime` raise `TypeError` / `ValueError`, so they take no `what`
  argument.
- `aclose()` delegates to `AlpacaHttp.aclose()`. A test asserts that both clients are closed
  afterwards (review M11).

- `connect`: the `ALLOW_LIVE` gate and the missing-keys check run before any client exists.
  Then `AlpacaHttp(...)` is built, and `GET /account` validates `crypto_status` and
  `crypto_tier` inside `try: ... except BaseException: await http.aclose(); raise`.
- Endpoints (template / path):
  - `/account`
  - `/positions`
  - `/assets/{symbol}` (path `/assets/BTC/USD`)
  - `/orders` (POST)
  - `/orders/{id}` (GET and DELETE)
  - `/orders:by_client_order_id` with `params={"client_order_id": cid}`
- Status contract (spec 3.3), one helper per method family:
  - Reads (`account`, `positions`, `asset`): a 2xx with a parsed body is parsed. Anything else
    raises `BrokerError(f"alpaca {what} returned {status} (request {rid})")`.
  - `submit_order`:
    - 200 with a parsed body goes through ack validation.
    - 403 with a parsed body raises `OrderRejected(f"alpaca rejected the order: {message}
      (request {rid})")`, with message `"<no message>"` when absent. `message` is
      `http.sanitize_message(...)` (Codex P2-1).
    - Anything else raises `BrokerError`.
  - `get_order`: 2xx is parsed; 404 raises `OrderNotFound(f"{order_id} (request {rid})")`;
    anything else raises `BrokerError`. `cancel_order`'s 404 raises the same. Callers branch
    only on the type (owner, after the dry run).
  - `get_order_by_client_id`: 2xx is parsed into a `BrokerOrderView`; 404 returns `None`;
    anything else raises `BrokerError`.
  - `cancel_order`: any 2xx returns; 404 raises `OrderNotFound`; anything else (422 included)
    raises `BrokerError`.
  - The status check comes first: `UnknownOrderStatus` from `order_state` propagates from all
    three order reads and from the ack.
- Order request JSON (spec 3.4):
  - Always `symbol`, `side`, `type`, `qty` (`wire_decimal`) and `time_in_force`.
  - `client_order_id` only when set.
  - `limit_price` (`wire_decimal`) only for limits.
- Ack validation (spec 3.4):
  - `id` must be a non-empty str.
  - `client_order_id` must equal ours when we sent one.
  - These response fields must equal the request: `_slash_symbol(symbol)`, `side`, `type`,
    `time_in_force`, `Decimal` `qty` (equality), and `limit_price` (equality; both null for
    market).
  - Any difference logs `alpaca.ack_mismatch` at error level, with `order_id` and
    `fields=[...]` (each `"name: sent != got"`), then raises. `got` and the response `id` pass
    through `http.diag(..., FIELD_RE)`; `sent` is ours (Codex r2). The raise is
    `BrokerError(f"alpaca ack does not match the order (request {rid})")`.
  - `OrderAck.status = order_state(status, filled_qty)`; `submitted_at` is `submitted_at`, or
    `created_at` if absent.
- Decimal parsing: one helper `_decimal(raw: object) -> Decimal` accepts only `str` (Review
  Focus 1) and is used for every money/qty field, reads included. Timestamps:
  `_aware(raw: object) -> datetime` uses `datetime.fromisoformat` and refuses naive values
  (Review Focus 3). Both run inside `_parsing`.
- `orders_enabled=False`: `submit_order` and `cancel_order` raise
  `OrderMethodsUnavailable("AlpacaBroker order writes are disabled until WP 1.6d")` before any
  I/O. `can_place_orders()` returns `self._orders_enabled`. `order_fills` keeps raising
  `OrderMethodsUnavailable("... until WP 1.6b")`.
- `asset_rules` cache: a dict of `symbol -> (expires_at, AssetRules)` on the injected
  monotonic clock. A failed fetch caches nothing.

- [ ] **Step 1: Write `tests/unit/broker/alpaca_mock.py`:**
  - `probe(step: str, *, run: str = "round1", n: int = 0) -> dict[str, Any]` returns the n-th
    logged entry for `step` from `tests/fixtures/alpaca/order_probes_20261112.json`.
  - `ACCOUNT` is the existing recorded account dict.
  - `order_from(body: dict[str, str]) -> BrokerOrder` builds the order a probe sent (Decimals
    from the strings).
  - `KEY`, `SECRET` (test constants).
  - `class Router`:
    - `add(method: str, path: str, status: int, json: Any = None, *, content: bytes | None = None,
      repeat: bool = False)`. `path` is the URL path including `/v2`, without the query. Each
      add queues one response; `repeat=True` serves it forever.
    - `.requests: list[httpx.Request]` records every request.
    - `.unrouted: list[str]`: a request with no queued response is recorded here as
      `"METHOD path"`, then raises `RuntimeError("unrouted ...")`. Every test that uses a Router
      ends with `assert router.unrouted == []`, so a mis-keyed route cannot pass as an
      `unknown` (review I6).
    - `.transport` is the `httpx.MockTransport` over it.
    - Every response carries `X-Request-ID: rid-<n>`, where n is the 1-based request count
      (dry run).
    - `add(..., raises: type[httpx.TransportError] | None = None)`: the route records the
      request, then raises that error (Task 6 and the ported transport-error tests). Tests
      that block use a plain `httpx.MockTransport(handler)` through
      `AlpacaBroker.connect(settings, transport=...)` or an `AlpacaHttp` built on it:
      cancellation and the connect-cleanup case (b) (dry run).
  - Connect tests build `Settings(_env_file=None, alpaca_api_key_id=KEY,
    alpaca_api_secret_key=SECRET)` (dry run).

  In `test_alpaca.py`, three module constants: `CHANGED: dict[str, str]` holds one wrong value
  per ack field (another client id, `"ETH/USD"`, `"sell"`, `"market"`, `"gtc"`, qty plus one
  increment, limit plus one cent). `CONTRACT` and `STATUS_TABLE` are lists with one row per row
  of the spec 3.3 and 3.4 tables (sketched below).

- [ ] **Step 2: Write the failing tests** (`test_alpaca.py`; named per acceptance item)

```python
async def broker(router: Router, *, orders_enabled: bool = True, **kw: Any) -> AlpacaBroker:
    http = AlpacaHttp(base_url=PAPER_URL, key_id=KEY, secret_key=SECRET, transport=router.transport)
    return AlpacaBroker(http, paper=True, fees=FEE_TIERS[1], orders_enabled=orders_enabled, **kw)


async def test_recorded_entry_and_exit_acks_pass_unchanged() -> None:
    for run, step in (
        ("round1", "E-entry"), ("round1", "H-exit"), ("round2", "I-sell-half"),
        ("round2", "C2-ioc-nofill"), ("round2", "D2-gtc-rest"),
    ):
        entry = probe(step, run=run)
        r = Router()
        r.add("POST", "/v2/orders", 200, entry["json"])
        ack = await (await broker(r)).submit_order(order_from(entry["body"]))
        assert (ack.order_id, ack.status) == (entry["json"]["id"], "new")
        assert json.loads(r.requests[0].content) == entry["body"]  # exact wire shape
        assert r.unrouted == []


@pytest.mark.parametrize(
    "field", ["client_order_id", "symbol", "side", "type", "time_in_force", "qty", "limit_price"]
)
async def test_each_ack_field_mismatch_raises_and_logs_the_broker_id(field: str) -> None:
    entry = probe("E-entry")
    r = Router()
    r.add("POST", "/v2/orders", 200, {**entry["json"], field: CHANGED[field]})
    with capture_logs() as logs, pytest.raises(BrokerError) as e:
        await (await broker(r)).submit_order(order_from(entry["body"]))
    assert type(e.value) is BrokerError
    (line,) = [x for x in logs if x["event"] == "alpaca.ack_mismatch"]
    assert line["order_id"] == entry["json"]["id"] and line["fields"][0].startswith(field)


async def test_get_order_with_another_id_is_a_broker_error() -> None:  # builder call, review I4
    resting = probe("D2-resting", run="round2")["json"]
    r = Router()
    r.add("GET", "/v2/orders/other-id", 200, resting)
    with pytest.raises(BrokerError, match="request"):
        await (await broker(r)).get_order("other-id")


@pytest.mark.parametrize("bad", ["../account", "", "a/b", "id?x=1", "id with space"])
async def test_unsafe_order_ids_are_refused_before_io(bad: str) -> None:  # review M6
    r = Router()
    b = await broker(r)
    for call in (b.get_order(bad), b.cancel_order(bad)):
        with pytest.raises(BrokerError):
            await call
    assert r.requests == []


async def test_writes_are_refused_before_io_while_disabled() -> None:
    resting = probe("D2-resting", run="round2")["json"]
    oid = resting["id"]
    r = Router()
    r.add("GET", f"/v2/orders/{oid}", 200, resting)
    b = await broker(r, orders_enabled=False)
    with pytest.raises(OrderMethodsUnavailable):
        await b.submit_order(order_from(probe("E-entry")["body"]))
    with pytest.raises(OrderMethodsUnavailable):
        await b.cancel_order(oid)
    assert b.can_place_orders() is False and r.requests == []
    assert (await b.get_order(oid)).status == "new"  # reads still work


def test_orders_enabled_with_a_live_account_is_refused() -> None:
    http = AlpacaHttp(base_url=LIVE_URL, key_id=KEY, secret_key=SECRET)
    with pytest.raises(BrokerError, match="paper accounts only"):
        AlpacaBroker(http, paper=False, fees=FEE_TIERS[1], orders_enabled=True)


async def test_asset_rules_are_cached_for_five_minutes() -> None:
    now = [0.0]
    r = Router()
    r.add("GET", "/v2/assets/BTC/USD", 200, probe("0-asset")["json"], repeat=True)
    b = await broker(r, monotonic=lambda: now[0])
    await b.asset_rules("BTC/USD")
    now[0] = 299.0
    await b.asset_rules("BTC/USD")
    assert len(r.requests) == 1
    now[0] = 300.0
    await b.asset_rules("BTC/USD")
    assert len(r.requests) == 2


async def test_aclose_closes_both_clients() -> None:  # review M11
    b = await broker(Router())
    await b.aclose()
    assert b._http.closed
```

Sketches (names and assertions binding):

```python
@pytest.mark.parametrize(("method", "status", "body", "raised"), CONTRACT)  # every 3.3 row
async def test_status_contract(...) -> None: ...
    # submit 403 json -> OrderRejected; submit 403 text -> BrokerError;
    # submit 400/401/404/422/429/500/503 -> BrokerError (exact class);
    # get_order 404 -> OrderNotFound; get_order 500 -> BrokerError;
    # by_client_id 404 -> None (raised=None, result None); cancel 204 -> None;
    # cancel 422 (probe G-cancel-final) -> BrokerError (exact);
    # cancel 404 (probe A-cancel-unknown-id) -> OrderNotFound; account 500 -> BrokerError.
    # Every BrokerError message from a response contains "request" and the X-Request-ID.

@pytest.mark.parametrize(("raw", "filled", "ours"), STATUS_TABLE)  # every 3.4 row
def test_status_mapping(raw: str, filled: str, ours: str) -> None:
    assert order_state(raw, Decimal(filled)) == ours

@pytest.mark.parametrize("raw", ["done_for_day", "replaced", "pending_replace", "stopped",
                                 "suspended", "calculated", "held", "accepted_for_bidding",
                                 "brand_new"])
def test_unlisted_statuses_raise(raw: str) -> None:
    with pytest.raises(UnknownOrderStatus) as e:
        order_state(raw, Decimal(0), "rid-9")
    assert e.value.status == raw and "rid-9" in str(e.value)

async def test_connect_closes_the_clients_when_validation_fails_or_is_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None: ...
    # monkeypatch `autotrader.broker.alpaca.AlpacaHttp` with a subclass that records its
    # instances. (a) crypto_status "INACTIVE" -> BrokerError; (b) a handler that blocks on an
    # event, the connect task cancelled after the request started. Each instance is closed.
```

Plus the Review Focus tests named above (JSON number, HTML 2xx, nanosecond/naive timestamps,
`BTCUSD` ack symbol, ack with `held`).

Plus `test_a_malformed_status_or_ack_leaks_no_secret_into_the_rendered_log` (Codex r2, sketch
with binding assertions). It uses `configure_logging(stream=buf)` as in the Task 4 rendered-log
test, with two cases:
1. `get_order` answers `status: f"held{SECRET}"`. The engine-side line
   `log.warning("risk.order_status_unknown", status=e.status)` is rendered.
2. A submit ack has `symbol: f"BTC/USD{SECRET}"`.

In both, `SECRET` and `KEY` are absent from the rendered output and from `str(exc)`, and
`<withheld` is present.

**Existing `test_alpaca.py` tests (review I8)**. Every one is kept, ported to the Router, or
dropped with its reason:

| Existing test | Fate |
|---|---|
| `test_satisfies_broker_protocol` | port (mypy structural check, now including `aclose`) |
| `test_account_values_are_exact_decimals` | port |
| `test_fee_schedule_follows_account_crypto_tier` | port |
| `test_unknown_crypto_tier_refuses_to_guess_fees` | port (and the clients end closed) |
| `test_inactive_crypto_account_is_refused` | port (folded into the connect-cleanup test's case (a) plus its own message check) |
| `test_live_mode_requires_allow_live` | port (no client is built) |
| `test_asset_rules_from_raw_strings` | port, on the recorded `0-asset` probe |
| `test_unknown_asset_is_a_broker_error` | port (404 from `/v2/assets/...`) |
| `test_crypto_positions_use_slash_symbols` | port |
| `test_order_methods_are_not_available_until_wp_1_6` | drop: replaced by `test_writes_are_refused_before_io_while_disabled` |
| `test_fee_tiers_match_published_schedule` | keep unchanged |
| `test_unmappable_position_symbol_is_a_broker_error` | port |
| `test_connect_without_keys_is_a_broker_error` | port (no client is built) |
| `test_connect_failures_are_broker_errors` | port: transport errors on `GET /account` during connect; `ConnectError` gives `RequestNotSent` (still a `BrokerError`) |
| `test_account_read_failures_are_broker_errors` | port, same parametrisation over the transport |
| `test_position_read_failures_are_broker_errors` | port, same |
| `test_error_messages_do_not_echo_sdk_messages` | port: a handler raising an httpx error whose message contains a marker string; the marker is not in `str(exc)` |
| `test_malformed_account_payload_is_a_broker_error` | port |
| `test_malformed_position_payload_is_a_broker_error` | port |
| `test_cancellation_is_not_swallowed` | port: a blocking handler, task cancelled, `CancelledError` propagates |
| `test_order_fills_is_not_implemented_until_wp_1_6` | port: `order_fills` raises `OrderMethodsUnavailable` mentioning 1.6b |
| `test_order_methods_raise_the_typed_unavailable_error` | drop: covered by the writes-disabled test |

- [ ] **Step 3: Run them to see them fail**

Run: `uv run pytest tests/unit/broker/test_alpaca.py -q`
Expected: FAIL (`AlpacaBroker` has no `http` constructor).

- [ ] **Step 4: Implement** per Interfaces. Delete `RawTradingClient`, `_make_client` and the
  `asyncio` import. Move `_call` into `broker/alpaca_data.py`, its only remaining user (dry
  run). Module docstring: the transport (`alpaca_http`, no SDK, no
  threads), the error contract by reference to the spec 3.3 table, writes refused until 1.6d,
  `order_fills` until 1.6b, and the `asset_rules` cache. `cli.py` still passes
  `AlpacaBroker.connect` (signature-compatible).
- [ ] **Step 5: Smoke tests (acceptance 16, review M11).** In
  `tests/smoke/test_alpaca_paper.py` the `broker` fixture becomes an async generator:
  `yield` the connected broker, then `await broker.aclose()`. Without that, a real run fails on
  `ResourceWarning`. Add two read-only tests that place no orders:
  - `test_unknown_order_id_is_not_found`: `get_order(str(uuid4()))` raises `OrderNotFound`;
  - `test_unknown_client_id_lookup_is_none`: `get_order_by_client_id(f"smoke-{uuid4()}")`
    returns `None`.
- [ ] **Step 6: Full check.** Expected: green. Also `uv run pytest -m smoke --collect-only -q`:
  the smoke module still imports.
- [ ] **Step 7: Commit**

```bash
git add src/autotrader/broker/alpaca.py src/autotrader/broker/alpaca_data.py tests/unit/broker/ tests/smoke/test_alpaca_paper.py
git commit -m "feat: implement Alpaca order methods on the owned transport, writes switched off (WP 1.6a)"
```

---

### Task 6: Engine-level acceptance with the real adapter

Acceptance 2 end to end: `RiskEngine` with `AlpacaBroker` over `MockTransport`. This catches
contract gaps between Tasks 3 and 5 that each task's own stubs cannot.

**Files:**
- Create: `tests/integration/test_alpaca_engine.py`

**Interfaces:**
- Consumes: `make_engine_`-style construction. Copy `db_url` (a **sync** fixture: `upgrade()`
  calls `asyncio.run`), `sessions` and `gate` from `tests/unit/risk/test_engine.py`; `exit()`
  and `sync_orders()` need no `FakeView` or baseline. Also `Router` with `raises=` (Task 5) and
  `AlpacaBroker(..., orders_enabled=True)`. Type the mixed `outcome` tuple as
  `tuple[Any, ...]` (dry run).

- [ ] **Step 1: Write the tests** (sketch: names and assertions binding). Setup reuses the
  construction from `test_exit_submits_a_namespaced_market_sell_of_the_free_qty`: its
  `ExitRequest` and the file's `orders(sessions)` helper (filter `side == "sell"`). The broker
  is a spy subclass that records what its `submit_order` raised, because the engine turns any
  `Exception` into `unknown`. Without the spy, a mis-keyed route or a parse bug would pass the
  `unknown` rows (review I6):

```python
class Spy(AlpacaBroker):
    raised: list[BaseException]
    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        try:
            return await super().submit_order(order)
        except BaseException as e:
            self.raised.append(e)
            raise

@pytest.mark.parametrize(("outcome", "row_status", "cause"), [
    (("status", 422, probe("F-duplicate-cid")["json"]), "unknown", BrokerError),
    (("status", 403, probe("B-below-min")["json"]), "rejected", OrderRejected),
    (("raise", httpx.ConnectError), "not_submitted", RequestNotSent),
    (("raise", httpx.ConnectTimeout), "not_submitted", RequestNotSent),
    (("raise", httpx.PoolTimeout), "not_submitted", RequestNotSent),
    (("raise", httpx.WriteError), "unknown", BrokerError),
    (("raise", httpx.ReadTimeout), "unknown", BrokerError),
    (("raise", httpx.RemoteProtocolError), "unknown", BrokerError),
])
async def test_submit_outcomes_through_the_engine(outcome, row_status, cause, ...) -> None:
    # account/positions/asset routes answer from the probes; POST /v2/orders answers `outcome`
    result = await engine.exit(request)  # exits need no feed; one held position
    (row,) = [o for o in await orders(sessions) if o.side == "sell"]
    assert (result.status, row.status) == (row_status, row_status)
    assert [type(e) for e in spy.raised] == [cause]  # exact class, not a subclass
    assert [r.method for r in router.requests if r.url.path == "/v2/orders"] == ["POST"]
    assert router.unrouted == []

async def test_an_unknown_status_row_becomes_an_unknown_status_incident(...) -> None:
    # Insert the row directly: tr.insert_row(..., status="new", broker_order_id=oid,
    # type_="limit", limit_price=...). A recorded ack carries the probe's client id, not
    # at-{jid}-o{row}, so engine.exit cannot produce it (dry run).
    # GET /v2/orders/{id} and the client-id lookup answer status "held"
    # (repeat=True). Pass 1 and a pass after order_resolution_timeout_s, each followed by
    # `ledger.apply_all(result)` so start_clock is persisted (review M9): the row's incident
    # is "unknown_status"; router.unrouted == [].
```

- [ ] **Step 2: Run them.** Expected: PASS without code changes. A failure means a contract
  gap: fix it in the task that owns the code (adapter or engine), not in the test.
- [ ] **Step 3: Full check, commit**

```bash
git add tests/integration/test_alpaca_engine.py
git commit -m "test: pin the submit and unknown-status outcomes through the engine with the Alpaca adapter (WP 1.6a)"
```

---

### Task 7: Lifecycle: startup check timeout, broker holder, teardown step, repair CLI

Spec 3.5 (startup timeout), 3.7 (startup and runtime ownership), acceptance 15 (runtime and CLI
part).

**Files:**
- Modify: `src/autotrader/core/runtime.py` (`_check_broker`, `run_core` holder, `_teardown`)
- Modify: `src/autotrader/cli_repair.py` (`_prepare`)
- Test: `tests/unit/core/test_runtime.py`, `tests/unit/test_cli_repair.py`

**Interfaces:**
- Produces: `BROKER_CHECK_S = 10.0`, and
  `async def _check_broker(connect_broker: BrokerConnect, settings: Settings, timeout_s: float =
  BROKER_CHECK_S) -> Any`. `connect()` and `get_account()` run under one
  `asyncio.timeout(timeout_s)`. If `get_account` fails, times out or is cancelled, the broker
  is closed first (`except BaseException: await broker.aclose(); raise`).
- Produces: in `run_core`, `holder = _BrokerHolder()` (dataclass, `broker: Any = None`) next to
  `supervisor`/`runner`. In `start()` the statement directly after `_check_broker` is
  `holder.broker = broker`, with no `await` in between. Every later use in `start()` reads the
  local.
- Produces: `_teardown(..., broker: Callable[[], Any] = lambda: None)`. A new step
  `bounded("broker", b.aclose())` with `b = broker()`, read **inside** the step and run only
  when not None. The step comes after `bus.close` and before `engine.dispose`. `run_core`
  passes `broker=lambda: holder.broker`.
- Produces: `cli_repair._prepare`: after a successful `connect_broker`, `prepare_link` runs in
  `try/finally` with `await broker.aclose()`.

- [ ] **Step 1: Write the failing tests** (sketch: names and assertions binding).
  `test_runtime.py` helpers:
  - `FakeBroker` gets a class attribute `closed = False`, which `aclose` sets on the instance.
    Its subclasses define their own `__init__` without calling `super()` (dry run).
  - Existing: `sim_connect(broker)` (the connect callable), `LiveBroker` (mode `"live"`),
    `make_control(timeout_s=...)`, `start_core`.
  - New: `GatedAccount(FakeBroker)`. Its first `get_account` call (the startup check) waits on
    an event; later calls answer at once.

```python
async def test_a_clean_shutdown_closes_the_broker(...) -> None:
    broker = FakeBroker()
    # start_core with connect_broker returning broker; request stop after the stream runs
    assert await task == 0 and broker.closed

async def test_a_mode_mismatch_closes_the_broker(...) -> None:
    broker = LiveBroker()  # mode() == "live" against a paper config
    with pytest.raises(RuntimeError, match="does not match"):
        await start_core(cfg, lock, control, connect_broker=sim_connect(broker))
    assert broker.closed

async def test_a_stop_during_the_backfill_closes_the_broker(...) -> None:
    # the MarketFeed.backfill monkeypatch of
    # test_startup_finishing_after_the_stop_request_starts_nothing (StopRaceSource fails the
    # stream instead) (dry run)
async def test_a_stop_while_the_check_blocks_then_succeeds_closes_the_broker(...) -> None:
    # GatedAccount: request stop while get_account waits; release it inside the shutdown
    # budget; startup returns None (stop requested); teardown's broker step closes it
async def test_a_failing_or_timed_out_startup_check_closes_the_broker(...) -> None:
    # get_account raises -> closed; timeout_s=0.01 with a get_account that waits on an
    # event never set -> TimeoutError and closed (outcome only, no timing assert)
async def test_a_forced_broker_close_makes_the_exit_unclean(...) -> None:
    # aclose waits on an event never set; make_control(timeout_s=0.5) so the deadline passes
    # quickly in real time (plus the 1 s engine.dispose floor, as in the existing forced-step
    # tests) -> "broker" in the forced steps, exit code EXIT_FAILED (review M12)
```

`test_cli_repair.py` (synchronous tests on the file's `Rig` / `rig` fixture, like
`test_link_mismatch_lists_the_difference` and `test_link_applies_and_records_the_operator`;
`connect_to(broker)` is the existing connect helper):

```python
def test_link_closes_the_broker_on_refusal_and_success(rig: Rig) -> None:
    # a SimBroker subclass whose aclose sets `closed` (`sim_with` gains a `cls` parameter);
    # a refused link (qty mismatch) and an applied link, on two client ids (rig.cid(1),
    # rig.cid(2)), each leave it closed. A failing connect has nothing to close (unchanged).
```

- [ ] **Step 2: Run them to see them fail**

Run: `uv run pytest tests/unit/core/test_runtime.py tests/unit/test_cli_repair.py -q -k "closes_the_broker or broker_close"`
Expected: FAIL.

- [ ] **Step 3: Implement** per Interfaces. Runtime module docstring: one sentence on broker
  ownership (connect, check, holder, teardown step `broker`) and the startup check timeout.
  `_teardown` docstring: the step order now includes `broker`. `cli_repair.py` module
  docstring: replace "The broker is not closed: the `Broker` protocol has no close ..." with
  "`link` closes the broker in a `finally` after the lookup (D22 (8))" (review M3; the Task 8
  patterns do not match this sentence).
- [ ] **Step 4: Full check.** Expected: green.
- [ ] **Step 5: Commit**

```bash
git add src/autotrader/core/runtime.py src/autotrader/cli_repair.py tests/unit/core/test_runtime.py tests/unit/test_cli_repair.py
git commit -m "feat: bound the startup broker check and close the broker's clients on every path (WP 1.6a)"
```

---

### Task 8: Docs alignment

Spec section 4, plus the user rule: correct every claim the change falsified, first.

**Files:**
- Modify: `SPEC.md`:
  - the §4.6 interface listing and "`AlpacaBroker` is currently read-only";
  - §5 step 6: "fill stream" becomes "fills are pulled per order (D17.3)";
  - line 7, "Implementation boundary": add a new dated "Update 2026-11-22 (WP 1.6a)" sentence
    and leave the old dated text untouched (dry run).
- Modify: `docs/runbook.md`, lines 12, 18, 26, 70, 145, 271, ~295 (`close_position`), 392 and
  433 as found today, plus the "Current limits (WP 1.5d-3)" header, which becomes 1.6a. Line 125
  ("Known costs (revisited in WP 1.6)") is still true and stays (dry run). Changes: "until WP 1.6"
  becomes 1.6d for orders and admission. `repair link` and the client-id lookup work
  from 1.6a. New `unknown_status` row in the incident table, with the spec 3.4 recovery
  procedure, plus its restart limitation: the evidence is kept in memory, so after a restart
  inside the resolution window an inconclusive lookup can end the row as `lookup_failed`.
  Before linking such a row, check the order's status at the broker and treat it as
  `unknown_status` if it is unmapped (Codex r2). `unknown_status` joins the list of linkable incidents. The `alpaca.http` line and
  where the request id appears.)
- Modify: `docs/alpaca-notes.md` (transport facts: alpaca-py has no timeout and retries POST,
  and trading calls no longer use it; rate limit 200/min; the request-budget numbers above; the
  httpcore facts 1-5 and the conditions (no proxy, HTTP/1.1, `retries=0`, anyio) with the
  httpx, httpcore and anyio versions; the connect timeout applying to TCP and TLS separately
  (`RequestNotSent` after up to 6 s); the closing network backend, why it exists (C1) and the
  private `_pool._network_backend` hook; "re-check all of this on any httpx, httpcore or
  anyio upgrade")
- Modify: `CLAUDE.md` (`run core` line: "no orders until WP 1.6" becomes 1.6d; "disabled with
  Alpaca until 1.6" becomes 1.6d)
- Modify: `PLAN.md` (dated 2026-11-22 note on the 1.6 row: 1.6a implemented, plan path; the
  two stale "Current repo state" sentences, "read-only `AlpacaBroker`" and "cannot place
  orders until 1.6" (dry run))
- Leave unchanged (checked in the dry run, still accurate or requirement text):
  - PLAN §3.2 items 3 and 5;
  - the WP 0.7 source list in alpaca-notes (`raw_data=True`);
  - the `core/ledger.py:555` comment;
  - the fake exception strings "until 1.6" in `test_engine.py` and `test_cli_repair.py`.

- [ ] **Step 1: Find falsified claims.** Run each and fix **every** hit that the change
  falsified (code comments included):

```bash
rg -n "until (WP )?1\.6\b|until WP 1\.6[^abcd]|read-only|to_thread|raw_data|TradingClient|close_position|close_all|stream_fills|redeliver|fill stream" --glob '!docs/superpowers/**' --glob '!uv.lock' .
rg -n --hidden "1\.6" CLAUDE.md AGENTS.md README.md docs/runbook.md 2>/dev/null
```

  Dated entries (PLAN decision history, review files, earlier specs and plans) stay as they
  are.
- [ ] **Step 2: Write the new surface** (runbook `unknown_status` procedure, alpaca-notes
  transport section).
- [ ] **Step 3: Full check** (ruff also lints docstrings in `src/`). Expected: green.
- [ ] **Step 4: Commit**

```bash
git add SPEC.md PLAN.md CLAUDE.md docs/runbook.md docs/alpaca-notes.md
git commit -m "docs: align SPEC, runbook, alpaca-notes and PLAN with the WP 1.6a order methods"
```

---

## Spec coverage

| Spec | Task |
|---|---|
| 1 scope items 1-5 | 4, 5 / 5 / 3, 4 / 2 / 1 |
| 3.1 transport, cache, logging | 4, 5 |
| 3.2 invariants, `RequestNotSent`, engine mapping | Pre-verification, 4, 3 |
| 3.3 error contract | 5, 6 |
| 3.4 mapping, ack, unknown status, `unknown_status` repair | 5, 3 |
| 3.5 reads, startup timeout | 5, 7 |
| 3.6 match check | 2 |
| 3.7 lifecycle | 1 (protocol), 5 (`connect`/`aclose`), 7 |
| 3.8 writes off | 5 |
| 3.9 protocol cleanup, SimBroker | 1 |
| 3.10 acceptance 1-16 | 1: 13, 14; 2: 11; 3: 2, 3, 5; 4: 6-9; 5: 1, 4, 5, 10, 12, 15, 16; 6: 2; 7: 15 |
| 4 docs | 8 (+ docstrings in each task) |
