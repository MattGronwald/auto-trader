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
one `alpaca.http` line per call. `broker/alpaca.py` keeps the endpoint semantics: the status-code
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
- Base URLs: paper `https://paper-api.alpaca.markets/v2`, live `https://api.alpaca.markets/v2`
  (live only through the existing `ALLOW_LIVE` gate in `connect`, unchanged).
- Headers `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY` are set once per client. Headers or keys
  never appear in a log line or an exception message.
- Decimals go out as `format(d, "f")`. Numbers in responses must be JSON strings; a JSON number
  where a decimal is expected raises `BrokerError` (Review Focus 1).
- Never branch on Alpaca's body `code` (spec 3.3).
- `orders_enabled` stays `False` everywhere outside tests. `connect()` has no parameter that can
  set it (spec 3.8).

### Builder calls made here (spec section 5)

- Module `broker/alpaca_http.py`; class `AlpacaHttp`; result type `HttpResult`.
- Pool sizes: read `httpx.Limits(max_connections=4, max_keepalive_connections=4)`, write
  `httpx.Limits(max_connections=4, max_keepalive_connections=0)`. Pool timeout 2 s.
- Cookies: `httpx.Cookies(http.cookiejar.CookieJar(policy=DefaultCookiePolicy(allowed_domains=[])))`.
  An empty allow list accepts no cookie (`DefaultCookiePolicy.is_not_allowed`); a test pins it.
- `asset_rules` cache TTL 300 s, on an injected monotonic clock.
- The startup broker check gets one overall `asyncio.timeout(BROKER_CHECK_S)` with
  `BROKER_CHECK_S = 10.0` in `core/runtime.py`, the engine's `io_timeout_s` default. The runtime
  does not configure `io_timeout_s`, so there is no config value to reuse.
- `aclose()` joins the `Broker` protocol. SimBroker and the runtime's test fakes get it.
- Log level of `alpaca.http`: `debug` for a 2xx GET, `info` for a 2xx POST/DELETE, `warning` for
  any non-2xx status or transport error. The spec fixes the line, not its level. At `debug`, a
  sync loop polling every 2 s does not flood the info log.
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
- **Peak:** 3 unsettled orders read every 2 s (`get_order`, 90/min), a flatten in progress
  (`sellable_positions` per 1 s Position Manager step, 60/min), plus submits and cancels. About
  **155-165 per minute**. Without the cache, every sync read and position read also fetches
  asset rules, which exceeds 200. `order_fills` (1.6b) adds a read only when `filled_qty`
  grew.

### Pre-implementation verification (spec 3.2, owner condition; done in the dry run)

Before Task 4 is written to disk, check these against the **installed** `httpcore` source and
record the result, with the versions, in `docs/alpaca-notes.md` (Task 8):

1. `httpcore/_async/connection.py` `AsyncHTTPConnection.handle_async_request`: `_connect()`
   (TCP, then `start_tls`) completes before the connection's `handle_async_request` writes the
   first request byte. `ConnectError` and `ConnectTimeout` are raised only inside `_connect()`
   (the `map_exceptions` around `connect_tcp` / `start_tls`).
2. `httpcore/_async/http11.py` `handle_async_request`: an exception during
   `_send_request_headers` / `_send_request_body`, `CancelledError` included, goes through the
   `except BaseException` path, which closes the connection
   (`_response_closed` under `AsyncShieldCancellation`). A cancelled write therefore leaves no
   connection that could finish the request later.
3. `httpcore/_async/connection_pool.py`: `PoolTimeout` is raised while waiting for a
   connection, before any request is assigned to one.
4. `httpx/_transports/default.py` maps `httpcore.ConnectError` / `ConnectTimeout` /
   `PoolTimeout` to the `httpx` exceptions of the same name and nothing else to them.

**If any of the four does not hold, stop and report to the owner before Task 4.** The
`RequestNotSent` mapping would be unsafe, and that changes the spec.

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
  order).
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

async def test_fills_hook_lists_every_fill_in_emission_order() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    a = await sim.submit_order(market("buy", "1"))
    b = await sim.submit_order(market("buy", "2"))
    sim.process_bar(bar(1, 100.0))
    assert [f.fill_id for f in sim.fills()] == [f"{a.order_id}:1", f"{b.order_id}:1"]
```

- [ ] **Step 2: Run them to see them fail**

Run: `uv run pytest tests/unit/broker/test_sim.py -k "alpaca_contract or removed_methods or fills_hook" -q`
Expected: 3 failed.

- [ ] **Step 3: Implement**
  - `base.py`: remove the three protocol methods and the now-unused `AsyncIterator` import;
    add `aclose`.
  - `sim.py`: remove `close_position`, `close_all`, `stream_fills`, `redeliver`, the `_fills`
    queue and its `put_nowait`. Keep `_fill_log`; `fills()` returns its values.
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
    rewriting them on `sell_free`. Stream-reading tests use `sim.fills()` (the hypothesis
    invariant test and `drain()` included). Tests that expect `OrderRejected` from cancelling a
    final order expect `BrokerError`. In `test_alpaca.py`, drop the removed names from the two
    "not implemented" tests. Add `async def aclose(self) -> None` to `test_runtime.FakeBroker`.
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

Plus one engine resolver test in `test_resolver.py`: a type (a) row whose broker view differs
only in `limit_price` becomes a `client_id_mismatch` incident. Plus one repair test in
`test_repair.py`: `prepare_link` refuses with "limit_price" in the reason.

- [ ] **Step 2: Run them to see them fail**

Run: `uv run pytest tests/unit/risk/test_resolve.py -q`
Expected: FAIL (`BrokerOrderView` rejects the new fields: `extra="forbid"`).

- [ ] **Step 3: Implement** the fields, `expected_tif`, the `mismatches` lines (after `qty`,
  before `created_at`), both callers, and SimBroker's view (`type`, `limit_price`,
  `time_in_force` from the stored `BrokerOrder`). Fix every test that builds a
  `BrokerOrderView` (mypy lists them). Update the `mismatches` docstring and the
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
  - `class UnknownOrderStatus(BrokerError)` with `__init__(self, status: str)`; stores
    `self.status`; message `f"unmapped alpaca order status {status!r}"`.
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

async def test_a_failure_after_the_write_stays_unknown(...) -> None:
    # submit raises BrokerError("... WriteError"): row "unknown"; a TimeoutError from _within too
```

`test_resolver.py`:

```python
async def test_an_unknown_status_read_makes_a_type_b_candidate_then_an_incident(...) -> None:
    # SimBroker subclass: get_order raises UnknownOrderStatus("held") for the row's broker id,
    # get_order_by_client_id raises UnknownOrderStatus("held") as well.
    first = await engine.sync_orders()
    assert row_id in first.failed
    assert [r.action for r in first.resolutions] == ["start_clock"]
    clock.advance(80)  # order_resolution_timeout_s
    later = await engine.sync_orders()
    (inc,) = [r for r in later.resolutions if r.action == "incident"]
    assert inc.reason == "unknown_status"
```

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

`test_repair.py`:

```python
async def test_link_of_an_unknown_status_incident_needs_a_final_broker_order(...) -> None:
    # row: broker id "b-1", incident "unknown_status"
    with pytest.raises(RepairRefused, match="broker lookup failed: UnknownOrderStatus"):
        await prepare_link(sessions, row, "b-1", raising(UnknownOrderStatus("held")))
    with pytest.raises(RepairRefused, match="broker order is not final: new"):
        await prepare_link(sessions, row, "b-1", returning(view(status="new")))
    plan = await prepare_link(sessions, row, "b-1", returning(view(status="canceled")))
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
- Modify: `pyproject.toml`, `uv.lock` (`uv add "httpx>=0.28.1,<0.29" "httpcore>=1.0.9,<1.1"`;
  httpcore is listed directly only to pin the verified minor)
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
```

- `transport=None` builds `httpx.AsyncHTTPTransport(limits=READ_LIMITS, retries=0)` for reads
  and `httpx.AsyncHTTPTransport(limits=WRITE_LIMITS, retries=0)` for writes. Both keep
  `verify=True`, and both clients get `trust_env=False`. A given `transport` (tests) serves
  both clients.
- `get` uses the read client; `post` and `delete` use the write client.
- Exception mapping (spec 3.2 table): `httpx.ConnectError`, `httpx.ConnectTimeout` and
  `httpx.PoolTimeout` raise `RequestNotSent(f"alpaca {method} {template} not sent:
  {type(e).__name__}")` from `e`. Any other `httpx.HTTPError` (or `httpx.InvalidURL`) raises
  `BrokerError(f"alpaca {method} {template} failed: {type(e).__name__}")` from `e`.
  `CancelledError` passes through. The message never includes `str(e)`.
- `alpaca.http` log line per call: `method`, `path` (the template), `order_id` (when given),
  `status` or `error` (exception type name), `elapsed_ms` (int, monotonic), `request_id`, and
  for non-2xx `message` (`body.get("message")` when the body is a dict). Levels as decided
  above.
- HTTP status codes are returned, never raised: the status contract lives in `alpaca.py`.

- [ ] **Step 1: Pre-implementation verification** (section above). Run
  `uv add "httpx>=0.28.1,<0.29" "httpcore>=1.0.9,<1.1"`, then read the four places in
  `.venv/lib/python3.12/site-packages/httpcore` and `httpx`. Record findings (file, function,
  version) for Task 8. Stop if any fails.

- [ ] **Step 2: Write the failing tests** in `tests/unit/broker/test_alpaca_http.py`

```python
def http(handler: Callable[[httpx.Request], httpx.Response | Awaitable[httpx.Response]], **kw: Any) -> AlpacaHttp:
    return AlpacaHttp(base_url=PAPER_URL, key_id=KEY, secret_key=SECRET,
                      transport=httpx.MockTransport(handler), **kw)

KEY, SECRET = "KEYID-not-a-real-key-7", "SECRET-not-a-real-secret-9"

async def test_clients_carry_the_specified_settings() -> None:
    h = http(lambda r: httpx.Response(200, json={}))
    for c in (h._read, h._write):  # private access in tests only
        assert c.timeout == httpx.Timeout(connect=3.0, write=5.0, read=5.0, pool=2.0)
        assert c.follow_redirects is False and c.trust_env is False
        assert c.headers["APCA-API-KEY-ID"] == KEY

async def test_a_set_cookie_is_not_kept() -> None:
    h = http(lambda r: httpx.Response(200, json={}, headers={"set-cookie": "a=b; Path=/"}))
    await h.get("/account", "/account")
    assert not h._read.cookies.jar and not h._write.cookies.jar  # nothing stored

@pytest.mark.parametrize("status", [429, 503])
async def test_no_retry_on_status(status: int) -> None:
    seen: list[httpx.Request] = []
    def handler(r: httpx.Request) -> httpx.Response:
        seen.append(r); return httpx.Response(status, json={"message": "x"})
    result = await http(handler).post("/orders", "/orders", {"symbol": "BTC/USD"})
    assert (result.status, len(seen)) == (status, 1)

@pytest.mark.parametrize(("exc", "raised"), [
    (httpx.ConnectError, RequestNotSent), (httpx.ConnectTimeout, RequestNotSent),
    (httpx.PoolTimeout, RequestNotSent), (httpx.WriteError, BrokerError),
    (httpx.WriteTimeout, BrokerError), (httpx.ReadError, BrokerError),
    (httpx.ReadTimeout, BrokerError), (httpx.RemoteProtocolError, BrokerError)])
async def test_transport_errors_map_by_whether_bytes_may_have_left(exc: type[Exception], raised: type[Exception]) -> None:
    seen: list[httpx.Request] = []
    def handler(r: httpx.Request) -> httpx.Response:
        seen.append(r); raise exc("boom", request=r)
    with pytest.raises(raised) as e:
        await http(handler).post("/orders", "/orders", {})
    assert type(e.value) is raised and len(seen) == 1  # exact class, no retry
    assert KEY not in str(e.value) and SECRET not in str(e.value)

def test_decimals_go_out_without_exponent() -> None:
    assert wire_decimal(Decimal("1E-7")) == "0.0000001"
    assert wire_decimal(Decimal("73667.30052")) == "73667.30052"

async def test_one_log_line_per_call_without_secrets() -> None:
    h = http(lambda r: httpx.Response(403, json={"message": "cost basis"}, headers={"X-Request-ID": "rid-1"}))
    with capture_logs() as logs:
        await h.post("/orders", "/orders", {})
        await h.get("/orders/{id}", "/orders/o-1", order_id="o-1")
    lines = [l for l in logs if l["event"] == "alpaca.http"]
    assert [(l["method"], l["path"], l.get("order_id"), l["status"]) for l in lines] == [
        ("POST", "/orders", None, 403), ("GET", "/orders/{id}", "o-1", 403)]
    assert lines[0]["request_id"] == "rid-1" and lines[0]["message"] == "cost basis"
    assert KEY not in repr(logs) and SECRET not in repr(logs)

async def test_a_handler_blocked_past_cancellation_records_nothing() -> None:
    started, release = asyncio.Event(), asyncio.Event()
    completed: list[httpx.Request] = []
    async def handler(r: httpx.Request) -> httpx.Response:
        started.set(); await release.wait(); completed.append(r); return httpx.Response(200, json={})
    h = http(handler)
    task = asyncio.create_task(h.post("/orders", "/orders", {}))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
    await asyncio.sleep(0)
    assert completed == []
    # same with asyncio.timeout rescheduled to "now" once started is set (the wait_for path)
```

Loopback tests (`tests/unit/broker/loopback.py` gives `SilentTls`, a server that records bytes,
never answers and sets `hello` on the first bytes and `closed` on EOF, and `KeepAliveHttp`, a
minimal HTTP/1.1 responder that counts accepted connections and answers each request with
`200 {}`):

```python
async def test_a_request_cancelled_in_tls_setup_writes_no_request_bytes() -> None:
    async with SilentTls() as srv:
        h = AlpacaHttp(base_url=f"https://127.0.0.1:{srv.port}/v2", key_id=KEY, secret_key=SECRET)
        task = asyncio.create_task(h.post("/orders", "/orders", {"symbol": "BTC/USD"}))
        await srv.hello.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await h.aclose()
        await srv.closed.wait()
    assert srv.record_types() == {0x16}  # TLS handshake records only, no application data
    assert b"POST" not in srv.received and SECRET.encode() not in srv.received

async def test_a_connect_timeout_and_a_refused_port_are_request_not_sent() -> None:
    async with SilentTls() as srv:
        h = AlpacaHttp(base_url=f"https://127.0.0.1:{srv.port}/v2", key_id=KEY, secret_key=SECRET,
                       timeout=httpx.Timeout(5.0, connect=0.05))
        with pytest.raises(RequestNotSent, match="ConnectTimeout"):
            await h.post("/orders", "/orders", {})
        await h.aclose()
    port = free_port()  # bound and closed again by the helper
    h = AlpacaHttp(base_url=f"http://127.0.0.1:{port}/v2", key_id=KEY, secret_key=SECRET)
    with pytest.raises(RequestNotSent, match="ConnectError"):
        await h.post("/orders", "/orders", {})
    await h.aclose()

async def test_writes_open_a_fresh_connection_reads_reuse_one() -> None:
    async with KeepAliveHttp() as srv:
        h = AlpacaHttp(base_url=f"http://127.0.0.1:{srv.port}/v2", key_id=KEY, secret_key=SECRET)
        await h.post("/orders", "/orders", {}); await h.post("/orders", "/orders", {})
        assert srv.connections == 2
        await h.get("/account", "/account"); await h.get("/account", "/account")
        assert srv.connections == 3
        await h.aclose()
    assert h.closed
```

- [ ] **Step 3: Run them to see them fail**

Run: `uv run pytest tests/unit/broker/test_alpaca_http.py -q`
Expected: FAIL (module missing).

- [ ] **Step 4: Implement `AlpacaHttp`** per Interfaces. One private `_send(client, method,
  template, path, **kw) -> HttpResult` does the timing, the mapping and the log line. Body:
  `parsed` is True only for a non-empty body that `response.json()` decodes; a decode error
  gives `body=None, parsed=False`, never an exception.
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

def order_state(raw: str, filled_qty: Decimal) -> OrderState: ...   # 3.4 table; raises UnknownOrderStatus
```

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
      (request {rid})")`, with message `"<no message>"` when absent.
    - Anything else raises `BrokerError`.
  - `get_order`: 2xx is parsed; 404 raises `OrderNotFound(order_id)`; anything else raises
    `BrokerError`.
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
    `fields=[...]` (each `"name: sent != got"`), then raises
    `BrokerError(f"alpaca ack does not match the order (request {rid})")`.
  - `OrderAck.status = order_state(status, filled_qty)`; `submitted_at` is `submitted_at`, or
    `created_at` if absent.
- Decimal parsing: one helper `_decimal(raw: object, what: str) -> Decimal` accepts only `str`
  (Review Focus 1) and is used for every money/qty field, reads included. Timestamps:
  `_aware(raw: object, what: str) -> datetime` uses `datetime.fromisoformat` and refuses naive
  values (Review Focus 3).
- `orders_enabled=False`: `submit_order` and `cancel_order` raise
  `OrderMethodsUnavailable("AlpacaBroker order writes are disabled until WP 1.6d")` before any
  I/O. `can_place_orders()` returns `self._orders_enabled`. `order_fills` keeps raising
  `OrderMethodsUnavailable("... until WP 1.6b")`.
- `asset_rules` cache: a dict of `symbol -> (expires_at, AssetRules)` on the injected
  monotonic clock. A failed fetch caches nothing.

- [ ] **Step 1: Write `tests/unit/broker/alpaca_mock.py`**: `probe(step: str, *, run: str =
  "round1", n: int = 0) -> dict[str, Any]` returns the n-th logged entry for `step` from
  `tests/fixtures/alpaca/order_probes_20261112.json`. `class Router` maps `(method,
  template-free path)` to a queued `httpx.Response`, records every request, and exposes
  `.transport`. `ACCOUNT` is the existing recorded account dict.
  `order_from(body: dict[str, str]) -> BrokerOrder` builds the order a probe sent (Decimals from
  the strings). In `test_alpaca.py`, `CHANGED: dict[str, str | None]` holds one wrong value per
  ack field (another client id, `"ETH/USD"`, `"sell"`, `"market"`, `"gtc"`, qty plus one
  increment, limit plus one cent).

- [ ] **Step 2: Write the failing tests** (`test_alpaca.py`; named per acceptance item)

```python
async def broker(router: Router, *, orders_enabled: bool = True, **kw: Any) -> AlpacaBroker:
    http = AlpacaHttp(base_url=PAPER_URL, key_id="k", secret_key="s", transport=router.transport)
    return AlpacaBroker(http, paper=True, fees=FEE_TIERS[1], orders_enabled=orders_enabled, **kw)

async def test_recorded_entry_and_exit_acks_pass_unchanged() -> None:
    for run, step in (("round1", "E-entry"), ("round1", "H-exit"), ("round2", "I-sell-half"),
                      ("round2", "C2-ioc-nofill"), ("round2", "D2-gtc-rest")):
        entry = probe(step, run=run)
        r = Router(); r.add("POST", "/v2/orders", 200, entry["json"])
        ack = await (await broker(r)).submit_order(order_from(entry["body"]))
        assert (ack.order_id, ack.status) == (entry["json"]["id"], "new")
        sent = json.loads(r.requests[0].content)
        assert sent == entry["body"]  # exact wire shape, decimals as plain strings

@pytest.mark.parametrize("field", ["client_order_id", "symbol", "side", "type", "time_in_force", "qty", "limit_price"])
async def test_each_ack_field_mismatch_raises_and_logs_the_broker_id(field: str) -> None:
    entry = probe("E-entry")
    bad = {**entry["json"], field: CHANGED[field]}  # CHANGED: one wrong value per field
    r = Router(); r.add("POST", "/v2/orders", 200, bad)
    with capture_logs() as logs, pytest.raises(BrokerError) as e:
        await (await broker(r)).submit_order(order_from(entry["body"]))
    assert type(e.value) is BrokerError
    (line,) = [l for l in logs if l["event"] == "alpaca.ack_mismatch"]
    assert line["order_id"] == entry["json"]["id"] and line["fields"][0].startswith(field)

@pytest.mark.parametrize(("method", "status", "body", "raised"), CONTRACT)  # every 3.3 row
async def test_status_contract(method: str, status: int, body: Any, raised: type[BaseException] | None) -> None: ...
    # CONTRACT rows: submit 403 json -> OrderRejected; submit 403 text -> BrokerError;
    # submit 400/401/404/422/429/500/503 -> BrokerError (exact class); get_order 404 -> OrderNotFound;
    # get_order 500 -> BrokerError; by_client_id 404 -> None (raised=None, result None);
    # cancel 204 -> None; cancel 422 (probe G-cancel-final) -> BrokerError (exact);
    # cancel 404 (probe A-cancel-unknown-id) -> OrderNotFound; account 500 -> BrokerError

@pytest.mark.parametrize(("raw", "filled", "ours"), STATUS_TABLE)  # every 3.4 row
def test_status_mapping(raw: str, filled: str, ours: str) -> None:
    assert order_state(raw, Decimal(filled)) == ours

@pytest.mark.parametrize("raw", ["done_for_day", "replaced", "pending_replace", "stopped", "suspended",
                                 "calculated", "held", "accepted_for_bidding", "brand_new"])
def test_unlisted_statuses_raise(raw: str) -> None:
    with pytest.raises(UnknownOrderStatus) as e:
        order_state(raw, Decimal(0))
    assert e.value.status == raw

async def test_writes_are_refused_before_io_while_disabled() -> None:
    r = Router(); r.add("GET", "/v2/orders/o-1", 200, probe("D2-resting", run="round2")["json"])
    b = await broker(r, orders_enabled=False)
    with pytest.raises(OrderMethodsUnavailable):
        await b.submit_order(order_from(probe("E-entry")["body"]))
    with pytest.raises(OrderMethodsUnavailable):
        await b.cancel_order("o-1")
    assert b.can_place_orders() is False and len(r.requests) == 0
    await b.get_order("o-1")  # reads still work

def test_orders_enabled_with_a_live_account_is_refused() -> None:
    with pytest.raises(BrokerError, match="paper accounts only"):
        AlpacaBroker(AlpacaHttp(base_url=LIVE_URL, key_id="k", secret_key="s"),
                     paper=False, fees=FEE_TIERS[1], orders_enabled=True)

async def test_asset_rules_are_cached_for_five_minutes() -> None:
    now = [0.0]
    r = Router(); r.add("GET", "/v2/assets/BTC/USD", 200, probe("0-asset")["json"], repeat=True)
    b = await broker(r, monotonic=lambda: now[0])
    await b.asset_rules("BTC/USD"); now[0] = 299.0; await b.asset_rules("BTC/USD")
    assert len(r.requests) == 1
    now[0] = 300.0; await b.asset_rules("BTC/USD")
    assert len(r.requests) == 2

async def test_connect_closes_the_clients_when_validation_fails_or_is_cancelled() -> None:
    # (a) crypto_status "INACTIVE" -> BrokerError; (b) a handler that blocks on an event, the
    # connect task cancelled after the request started. In both, the AlpacaHttp the connect
    # built is closed: monkeypatch `autotrader.broker.alpaca.AlpacaHttp` with a subclass that
    # records its instances, then assert `instance.closed`.
```

Plus the Review Focus tests named above (JSON number, HTML 2xx, nanosecond/naive timestamps,
`BTCUSD` ack symbol, ack with `held`), and the read-side tests ported from the old file
(account Decimals, crypto positions only, `BTCUSD` positions, asset rules, unknown
`crypto_tier`, missing keys, `ALLOW_LIVE`).

- [ ] **Step 3: Run them to see them fail**

Run: `uv run pytest tests/unit/broker/test_alpaca.py -q`
Expected: FAIL (`AlpacaBroker` has no `http` constructor).

- [ ] **Step 4: Implement** per Interfaces. Delete `RawTradingClient`, `_make_client`, `_call`
  and the `asyncio` import. Module docstring: the transport (`alpaca_http`, no SDK, no
  threads), the error contract by reference to the spec 3.3 table, writes refused until 1.6d,
  `order_fills` until 1.6b, and the `asset_rules` cache. `cli.py` still passes
  `AlpacaBroker.connect` (signature-compatible).
- [ ] **Step 5: Full check.** Expected: green. Also `uv run pytest -m smoke --collect-only -q`:
  the smoke module still imports.
- [ ] **Step 6: Commit**

```bash
git add src/autotrader/broker/alpaca.py tests/unit/broker/ tests/smoke/test_alpaca_paper.py
git commit -m "feat: implement Alpaca order methods on the owned transport, writes switched off (WP 1.6a)"
```

---

### Task 6: Engine-level acceptance with the real adapter

Acceptance 2 end to end: `RiskEngine` with `AlpacaBroker` over `MockTransport`. This catches
contract gaps between Tasks 3 and 5 that each task's own stubs cannot.

**Files:**
- Create: `tests/integration/test_alpaca_engine.py`

**Interfaces:**
- Consumes: `make_engine_`-style construction (copy the minimal parts from
  `tests/unit/risk/test_engine.py`: sessions fixture, gate, baseline, `FakeView`), `Router`
  (Task 5), `AlpacaBroker(..., orders_enabled=True)`.

- [ ] **Step 1: Write the tests**

```python
@pytest.mark.parametrize(("outcome", "row_status"), [
    (("status", 422, probe("F-duplicate-cid")["json"]), "unknown"),
    (("status", 403, probe("B-below-min")["json"]), "rejected"),
    (("raise", httpx.ConnectError), "not_submitted"),
    (("raise", httpx.ConnectTimeout), "not_submitted"),
    (("raise", httpx.PoolTimeout), "not_submitted"),
    (("raise", httpx.WriteError), "unknown"),
    (("raise", httpx.ReadTimeout), "unknown"),
    (("raise", httpx.RemoteProtocolError), "unknown"),
])
async def test_submit_outcomes_through_the_engine(outcome, row_status, ...) -> None:
    # account/positions/asset routes answer from the probes; POST /v2/orders answers `outcome`
    result = await engine.exit(exit_request(BTC))   # exits need no feed; one held position
    (row,) = await sell_rows(sessions)
    assert (result.status, row.status) == (row_status, row_status)

async def test_an_unknown_status_row_becomes_an_unknown_status_incident(...) -> None:
    # row with broker id; GET /v2/orders/{id} and the client-id lookup answer status "held";
    # sync_orders twice around order_resolution_timeout_s -> incident reason "unknown_status"
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

- [ ] **Step 1: Write the failing tests** (`test_runtime.py`; `FakeBroker` gets `closed: bool`
  set by `aclose`, and `GatedAccount(FakeBroker)` whose first `get_account` call (the startup
  check) waits on an event)

```python
async def test_a_clean_shutdown_closes_the_broker(...) -> None:
    broker = FakeBroker()
    # start_core with connect_broker returning broker; request stop after the stream runs
    assert await task == 0 and broker.closed

async def test_a_mode_mismatch_closes_the_broker(...) -> None:
    broker = LiveFakeBroker()  # mode() == "live" against a paper config
    with pytest.raises(RuntimeError, match="does not match"):
        await start_core(cfg, lock, control, connect_broker=returning(broker))
    assert broker.closed

async def test_a_stop_during_the_backfill_closes_the_broker(...) -> None:  # StopRaceSource-style
async def test_a_stop_while_the_check_blocks_then_succeeds_closes_the_broker(...) -> None:
    # GatedAccount: request stop while get_account waits; release it inside the shutdown
    # budget; startup returns None (stop requested); teardown's broker step closes it
async def test_a_failing_or_timed_out_startup_check_closes_the_broker(...) -> None:
    # get_account raises -> closed; timeout_s=0.01 with a get_account that waits on an
    # event never set -> TimeoutError and closed (outcome only, no timing assert)
async def test_a_forced_broker_close_makes_the_exit_unclean(...) -> None:
    # aclose waits on an event never set; teardown deadline passes -> "broker" in the
    # forced steps, exit code EXIT_FAILED
```

`test_cli_repair.py`:

```python
async def test_link_closes_the_broker_on_refusal_and_success(...) -> None:
    # a SimBroker subclass recording aclose; a refused link (no match) and an applied link
    # each leave it closed
```

- [ ] **Step 2: Run them to see them fail**

Run: `uv run pytest tests/unit/core/test_runtime.py tests/unit/test_cli_repair.py -q -k "closes_the_broker or broker_close"`
Expected: FAIL.

- [ ] **Step 3: Implement** per Interfaces. Runtime module docstring: one sentence on broker
  ownership (connect, check, holder, teardown step `broker`) and the startup check timeout.
  `_teardown` docstring: the step order now includes `broker`.
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
- Modify: `SPEC.md` (§4.6 interface listing and "`AlpacaBroker` is currently read-only"; §5
  step 6 "fill stream" becomes "fills are pulled per order (D17.3)")
- Modify: `docs/runbook.md` (lines 12, 18, 26, 70, 125, 145, 271, 392, 433 as found today: "until
  WP 1.6" becomes 1.6d for orders and admission. `repair link` and the client-id lookup work
  from 1.6a. New `unknown_status` row in the incident table, with the spec 3.4 recovery
  procedure. `unknown_status` joins the list of linkable incidents. The `alpaca.http` line and
  where the request id appears.)
- Modify: `docs/alpaca-notes.md` (transport facts: alpaca-py has no timeout and retries POST,
  and trading calls no longer use it; rate limit 200/min; the request-budget numbers above; the
  httpcore verification results with versions and "re-check on httpx/httpcore upgrade")
- Modify: `CLAUDE.md` (`run core` line: "no orders until WP 1.6" becomes 1.6d; "disabled with
  Alpaca until 1.6" becomes 1.6d)
- Modify: `PLAN.md` (dated 2026-11-22 note on the 1.6 row: 1.6a implemented, plan path; the
  "Current repo state" sentence on `AlpacaBroker`)

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
