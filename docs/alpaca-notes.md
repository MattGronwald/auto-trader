# Alpaca crypto — verified facts (WP 0.7)

Verified **2026-10-11** against the docs and the paper API (paper account, `alpaca-py` 0.44.0).
Re-verify before Phase 5. Alpaca notes these values "could change".

Sources:
- Crypto trading docs: <https://docs.alpaca.markets/docs/crypto-trading>
- Paper API, read-only calls: `get_account`, `get_all_positions`, `get_asset` (`raw_data=True`)

## G2 — order types: no brackets

- Crypto supports **market, limit and stop-limit** orders only. Time in force: `gtc`, `ioc`.
- **No bracket, OCO, OTO or trailing orders** are listed for crypto.
- **Consequence:** stops stay client-side in the Position Manager, as planned. A broker-side stop-limit could act as a backup stop. The docs don't describe its trigger behaviour, and a stop-limit can fail to fill in a gap, so it is no substitute. Decide in WP 1.5 after testing it on paper.

## G10 — fee mechanics: fee comes out of what you receive, posted end of day

- "The crypto fee will be charged on the credited crypto asset/fiat (what you receive) per trade":
  - **buy BTC/USD:** fee in **BTC** (the position ends up smaller than the ordered qty);
  - **sell BTC/USD:** fee in **USD**.
- "Fees are currently calculated and **posted end of day**". Same-day queries may return nothing. Alpaca says real-time posting is planned.
- Fees are retrievable only as account **activities** (`activity_type` `CFEE` / `FEE`). The docs don't say whether order or fill objects carry fees.
- **Paper observation (2026-10-12, fee check step 2):** no `CFEE`/`FEE` activity was observed in the queries made: ~32 h after the buy, and right after the sell (not yet after an end-of-day for the sell). So far the fee was visible only as the coin shortfall (buy) and the cash shortfall (sell). Whether, and when, paper or live posts these activities is unverified.
- **Paper observation (2026-10-14, re-query):** paper **did** post a `CFEE` for each leg, both with activity `date` 2026-10-12. Same-scope queries on 2026-10-12 (17:59Z, and a few seconds after the 18:03Z sell) returned none; the 2026-10-14 query returned both. The exact posting/visibility time is unverified. "Posted end of day" remains Alpaca's documented claim, not an observed paper rule (see the re-query section). Live is still unverified.

**Consequences:**
1. **Until WP 1.R2, `SimBroker` charged all fees in USD.** That is right for sells but wrong for buys, where the real position is smaller than the ordered qty. Owner decision D10 (2026-10-11 review) selects coin-denominated buy fees and dust handling; PLAN 1.R2 implements it. The simulator had not yet been changed then. **2026-10-12 (WP 1.R1b):** `FeeSchedule.buy_fee_asset` now states the denomination: `AlpacaBroker` reports `base`, and SimBroker reports `quote` and refuses `base` until 1.R2. Risk sizing and expectancy use the matching cash-flow equation. **2026-10-12 (WP 1.R2):** SimBroker now models the coin fee (rounded up to 1e-9) and accepts buy_fee_asset="base".
2. **Reconciliation (WP 1.5):** the empirical step-1 probe below shows the buy fee deducted from coin **at fill time**; the sell fee likewise comes out of the USD proceeds at fill (step 2). No fee activity record was observed in the recorded paper queries. Gross fill qty and net broker position therefore differ immediately. Explain that difference using fee denomination and bounded rounding; an unexplained discrepancy still pauses new entries. Do not hide discrepancies by unconditionally overwriting local qty.
3. **Journal (WP 1.5 basic accounting; WP 3.1 activity reconciliation):** the posted fee record is not available at fill time. Record an **estimated** fee from `fee_schedule()` at fill time, then reconcile it with the posted `CFEE` amount once available, retaining its asset denomination and avoiding double counting.
   **2026-10-12 (corrected after the PR #16 post-merge review):** no fee activity has been observed on paper so far, so accounting must not block on one. Activity reconciliation stays in scope for both paper and live, and must not double count against the coin/cash deductions already applied.
   - **A gross-minus-net residual is not the fee.** The residual is gross fill qty minus net received qty (buys), or gross fill notional minus cash credited (sells). It mixes fee and rounding. In the step-2 sell, the residual is 0.0305 USD; at 25 bps the fee would be 0.0243, and the other 0.0063 would be rounding.
   - **It is only per-order in isolated probes.** Account cash and position qty are aggregates, so another fill between snapshots contaminates the delta.
   - **So:** record fee and rounding as estimates, with provenance (schedule rate, which snapshot) and bounds. Finalise fees/P&L only after fill-linked reconciliation of all relevant cash and position changes.
   - **2026-10-14:** posted `CFEE` records do arrive on paper (re-query section). Reconcile the fill-time estimate against them, keyed on the activity `id`. The buy record carries the coin `qty` and `price`, with `net_amount` 0.
   - Don't use Alpaca's position P&L for this either: `cost_basis` is net qty × entry price, so the buy fee doesn't appear in `unrealized_pl` at all.
4. **Exits sell the actual position qty**, not the ordered entry qty, or they can hit "insufficient qty" immediately after the fill. Account for reserved sell qty as well; test the actual close/dust behavior before relying on retries.

## Fees

Volume tiers (30-day volume in USD → maker / taker bps):

| Tier | 30D volume (USD) | Maker | Taker |
|---|---|---|---|
| 1 | 0 – 100k | 15 | 25 |
| 2 | 100k – 500k | 12 | 22 |
| 3 | 500k – 1M | 10 | 20 |
| 4 | 1M – 10M | 8 | 18 |
| 5 | 10M – 25M | 5 | 15 |
| 6 | 25M – 50M | 2 | 13 |
| 7 | 50M – 100M | 2 | 12 |
| 8 | 100M+ | 0 | 10 |

- The account reports its tier as `crypto_tier` (paper account: **1**). `AlpacaBroker.fee_schedule()` maps it via `FEE_TIERS`. An unknown tier refuses to connect rather than guess.
- At ~1,000 EUR the PoC stays in tier 1. A round trip with market orders costs **~50 bps** (25 in, 25 out). That is the bar the Risk Engine's expectancy check (§4.5 #11) has to clear.

## Asset rules (paper API, 2026-10-11)

| Symbol | `min_order_size` | `min_trade_increment` | `price_increment` |
|---|---|---|---|
| BTC/USD | 0.000012054 | 0.000000001 | 0.000000001 |
| ETH/USD | 0.000398852 | 0.000000001 | 0.000000001 |
| SOL/USD | 0.009084302 | 0.000000001 | 0.000000001 |

- **The docs' BTC example (`0.0001` / `0.0001` / `1`) is outdated.** Use the API values.
- `min_order_size` differs per asset and looks like **~1 USD at current prices**. This is inferred, not documented, so it likely moves with price. Fetch it at sizing time; don't cache it for long. *(WP 1.6a: `AlpacaBroker` caches it for 5 minutes, see "Trading transport".)*
- **No minimum notional is published.** There is a **maximum of $200k notional per order**, irrelevant at PoC size. `AssetRules.min_notional` is therefore `0` for Alpaca.
  *Corrected 2026-11-12 (WP 1.6a probes):* paper refuses a **buy** worth under $10 (403, "cost basis must be >= minimal amount of order 10"), measured at qty × limit price; market sells under $10 were accepted. See "Order endpoint probes (WP 1.6a)". `AssetRules.min_notional` is still `0` for Alpaca until 1.6c.
- `price_increment` is mapped into `AssetRules.price_increment` (WP 1.3) and used to round the marketable entry limit up.
- **The API returns these as strings.** The `alpaca-py` models turn them into floats, so `AlpacaBroker` read them with `raw_data=True` as exact `Decimal`s. *Since WP 1.6a (2026-11-22) the trading calls no longer use `alpaca-py`: `AlpacaBroker` reads the JSON itself and requires these values to be JSON strings (see "Trading transport"). `raw_data=True` still applies to market data.*

## Account

- Paper account: `status` ACTIVE, `crypto_status` ACTIVE, currency USD, $100,000 paper cash.
- No margin and no shorting for crypto (`marginable`/`shortable` false). Buys are checked against `non_marginable_buying_power`.

## Empirical fee check — step 1 (2026-10-11, paper)

Owner-approved paper order: market buy, notional $10 BTC/USD, `gtc`.

| | Value |
|---|---|
| Fill | 0.000118113 BTC @ 82,994.20 (`filled_avg_price`) |
| Cash debited | 9.80 USD (= fill qty × price, rounded to cents) |
| Position qty / `qty_available` | **0.000117817** = fill qty − **25.06 bps** |
| Position symbol | **`BTCUSD`** (FILL activity: `BTC/USD`) |
| Fee fields on order | none |
| `CFEE`/`FEE` activity | none yet (same day) |

- **The buy fee comes out of the coin at fill time.** The position is short by the taker fee right away; it isn't deducted at end of day. Only the `CFEE` *record* is posted later.
- **The cash side carries no fee on a buy.** It's exactly the fill value.
- **A $10 notional market order used only ~$9.80.** That's probably a buffer for crypto notional orders (inference). The Risk Engine submits `qty` orders, so this doesn't affect us.
- **Crypto positions report the symbol without a slash.** `AlpacaBroker`'s normalisation is required.

**Step 2 (due 2026-10-12 or later):** re-read the position and the `CFEE` activity, then sell the full position qty and record the USD fee and its posting.

## Empirical fee check — step 2 (2026-10-12, paper)

Owner-approved paper order: market sell of the full position, `qty` 0.000117817 BTC/USD, `gtc`. Raw payloads (order ids replaced): `tests/fixtures/alpaca/fee_check_btcusd_20261012.json`.

| | Value |
|---|---|
| Pre-sell read (~32 h after the buy) | position unchanged (0.000117817, `cost_basis` 9.778128 = net qty × entry); no `CFEE`/`FEE` activity |
| Fill | 0.000117817 BTC @ 82,420.56, filled in ~2 ms, `position_intent` `sell_to_close` |
| Gross notional | 9.71054 USD |
| Cash credited | **9.68** USD (99,990.20 → 99,999.88) |
| Gross − cash credit | 0.0305 USD (31.5 bps of gross). This is fee plus rounding, not separable from one sample; see below |
| Position after | none (full close, no dust) |
| `CFEE`/`FEE` activity | none, queried right after the sell |
| Round trip | −0.12 USD on ~$9.80 (fees ≈ 0.025 + 0.024, rest is price move and rounding) |

- **The sell fee comes out of the USD proceeds at fill time.** The FILL activity shows the gross price, and nothing records the fee.
- **Cash looks rounded down to cents on both legs.** 25 bps of the gross gives 9.6863. That truncates to 9.68 but rounds half-up to 9.69. The buy also truncated (9.8027 → 9.80). "25 bps taker, cash truncated to cents" explains both legs. On a $10 trade one cent is ~10 bps, so a larger trade would be needed to separate this from a slightly higher fee rounded normally (inference, not verified).
- **Selling the exact `qty_available` closes the position cleanly.** After a coin-fee buy, the net qty (9 decimals) was accepted as-is, with no increment rejection and no dust left behind.
- **No `CFEE`/`FEE` activity observed yet:** none ~32 h after the buy, none right after the sell. A later posting for either leg isn't ruled out, so accounting must not block on one (see G10 consequence 3). *(Bounded 2026-10-12 after the PR #16 post-merge review; the first version claimed paper never posts these.)* **Resolved 2026-10-14:** both were posted with `date` 2026-10-12 (re-query section).

## Fee check re-query (2026-10-14, paper, read-only)

Raw payloads (order ids replaced): `tests/fixtures/alpaca/fee_check_cfee_20261014.json`. No orders were placed.

| | Value |
|---|---|
| Buy `CFEE` | `qty` **-0.000000296** BTC, `price` 82994.2, `net_amount` 0, "Coin Pair Transaction Fee (Non USD)", symbol `BTCUSD`, `date` 2026-10-12, `created_at` 2026-10-11T10:07:01Z |
| Sell `CFEE` | `net_amount` **-0.03** USD, "Coin Pair Transaction Fee (USD)", `date` 2026-10-12, `created_at` 2026-10-12T18:03:03Z |
| `FEE` activities | none |
| All activities on the account | 5: `JNLC` +100000 (2026-10-09), two `FILL`, two `CFEE` |
| Orders since 2026-10-10 | only the two fee-check orders |
| Cash | **99980.04**; positions none |

**Paper posts a `CFEE` per leg.** Each leg has one record, dated 2026-10-12, the buy's dated later than its 2026-10-11 trade and the sell's dated the same day as its trade.
- **Visibility, as observed:** `GET /account/activities/CFEE` with `after=2026-10-10T00:00:00Z` (default page; at most 2 results, well inside one page) returned nothing at 2026-10-12 17:59Z, nothing a few seconds after the 18:03:03Z sell, and both records at 2026-10-14 16:37Z.
- **Timing is unverified.** `created_at` is 4.9 s after the buy fill and 0.37 s after the sell fill, but neither field says when a query could first return the record. All that was observed is earlier absence and later presence, not that posting happens after the trade day.

 The buy record's coin qty, 2.96e-7, is the taker fee **rounded up** to 1e-9 (2.952825e-7 exact), the rule SimBroker uses since WP 1.R2. The sell record of -0.03 USD is the 25 bps fee on 9.7105 (0.0243) rounded to a cent.

**Cash doesn't reconcile.**
- **Observed change:** cash was **99999.88** right after the sell (2026-10-12 18:03Z) and **99980.04** at 2026-10-14 16:37Z: **-19.84 USD**, with no position.
- **Activity baseline:** replaying the gross `FILL` amounts and every cash-bearing activity once gives 100000 - 9.8026939446 (buy, 0.000118113 x 82994.2) + 9.71054311752 (sell, 0.000117817 x 82420.56) - 0.03 (sell `CFEE`) = **99999.8778**. That rounds to the post-sell 99999.88. The buy `CFEE` is in coin (`net_amount` 0) and moves no cash.
- So the activities explain the post-sell balance, and nothing on the account explains the later -19.84.
- The cause is unknown (one observation). It could be a paper-account artifact or a movement the activities API doesn't list. Don't infer more.
- This is the kind of unexplained cash change WP 1.5/1.6 reconciliation must detect, and it must pause entries on it.
- **The sell fee was charged once, as the `CFEE`.** The 9.68 credit seen in step 2 is the gross 9.71 less this -0.03 record, not a separate deduction. A reconciliation that subtracted the `CFEE` from the already-netted cash delta would double count it.
- **Alternative reading of the step-2 "cash truncated to cents" note (one sample, inference):** the data also fit gross amounts rounded to cents (9.8027 -> 9.80, 9.7105 -> 9.71) plus a sell fee rounded *up* to a cent (0.0243 -> 0.03). The two readings can't be told apart from one fill.

## Order endpoint probes (WP 1.6a, 2026-11-12, paper)

Two owner-run probe runs against the paper trading API, plain HTTP with 3 s connect / 5 s read timeouts and no retries. One sample per symbol (BTC/USD, ETH/USD, SOL/USD) per probe, about $15 per order; every position was closed again. Raw responses (order ids replaced): `tests/fixtures/alpaca/order_probes_20261112.json`.

| Probe | Observed (all three symbols unless noted) |
|---|---|
| Marketable IOC limit buy (entry shape) | 200, ack status `pending_new`; `qty`, `limit_price`, `type`, `time_in_force` echoed **exactly** as sent; `filled` on the first lookup (fill ~2 ms after creation) |
| Market sell of the full `qty_available` (exit shape) | 200, `qty` echoed exactly; filled; no position left (no dust) |
| Client-id lookup after submit | found on the **first** lookup, 0.22–0.30 s after the POST was sent |
| Same `client_order_id` submitted again | **422** "client_order_id must be unique", no order in the body. The body's `code` differed by symbol (`40010001` BTC/SOL, `42210000` ETH) |
| Buy worth under $10 at its limit price | **403** `40310000` "cost basis must be >= minimal amount of order 10" (also for qty below `min_order_size`: the $10 check answers first) |
| Market sell worth about $6 (partial, then the full remaining holding) | accepted and filled; the holding closed with nothing left |
| Non-marketable IOC limit buy | ack `pending_new`, final **`canceled`** (`canceled_at` set, `expired_at` null), zero fill |
| Resting GTC limit buy | `pending_new` → `new`; cancel **204**, status `canceled` on the next read (no `pending_cancel` seen); cancelling it again **204** |
| Cancel of a filled order | **422** `42210000` "order is already in \"filled\" state" |
| Unknown order id (get, cancel) and unknown client id (lookup) | **404** `40410000` "order not found for ..." |

- **The $10 minimum applies to buys, measured at the limit price.** A $15 qty priced at half the market was refused. `min_order_size` (about $1) never decides. Market sells of about $6 were not refused, so in these samples a holding under $10 stays sellable. Limit sells were not probed. *Bounded:* one sample per symbol, paper only.
- **A duplicate client id is refused with 422 and does not return the existing order.** The first order is unaffected. The error `code` is not stable across requests, so the adapter must not branch on it.
- **Status codes, not body codes, carry the meaning.** 403 and 422 on submit were refusals (no order created). 404 means unknown. Cancel is idempotent for a cancelled order (204) but not for a filled one (422).
- **Visibility within 0.3 s is three samples, not a guarantee.** The 60 s `order_visibility_allowance_s` stays an operational bound.

## Trading transport (WP 1.6a, verified 2026-11-22)

Since WP 1.6a every trading-API call (orders, account, positions, asset rules) goes through
`broker/alpaca_http.py` on `httpx`. `alpaca-py` stays for market data only (until WP 1.6c).

**Why not `alpaca-py` for trading.** Its `TradingClient` (0.44) is built on `requests` and sends
every request without a timeout; `TradingClient.__init__` exposes no timeout or retry setting, so
neither can be changed without writing private attributes. It retries 429 and 504 (including
POST). A retried `POST /orders` or a hang
the engine's I/O budget cannot cut off defeats the "not sent" argument (SPEC §4.6, PLAN D22).
Its sync calls also needed `to_thread`, which cannot be cancelled once running. The new transport
has fixed timeouts, no retries and no threads.

**Settings.** `httpx.Timeout(connect=3, write=5, read=5, pool=2)`. The connect timeout applies to
TCP and to the TLS handshake **separately**, so a connect that never completes raises
`RequestNotSent` after up to 6 s (3 s TCP + 3 s TLS), under the engine's 10 s budget. Read client: keep-alive
pool, 4 connections; write client (`POST /orders`, `DELETE /orders/{id}`): 4 connections, no
keep-alive, a fresh connection per write. `retries=0`, no redirects, `trust_env=False`, TLS
verification on, no cookies kept.

**Rate limit and request budget.** Alpaca's trading API allows 200 requests per minute. The 2 s
order-sync loop and the Position Manager call `asset_rules` per symbol, so uncached, three
unsettled orders on three symbols already use about 180 of the 200. `AlpacaBroker` caches
`asset_rules` per symbol for 5 minutes (`ASSET_RULES_TTL_S`); the $10 buy minimum, not
`min_order_size`, decides what is orderable, so the cache is safe. 1.6d has to measure the real
request rate before enabling writes (PLAN 1.6 row).

**Response values.** Decimals must be plain decimal strings (`-?digits(.digits)?`, the form every
recorded response uses; whitespace, `_`, a `+`, an exponent, NaN or Infinity fail the read) and in
range: equity, a position's qty and entry price at least 0, `qty_available` between 0 and `qty`,
one position row per symbol, an order's `qty` and `filled_avg_price` above 0, `filled_qty` at most
`qty`. Account `cash` is not range-checked (a margin account can show it below zero). A value
out of range is a `BrokerError` naming the request id, never the value (Fable review 2 M2,
2026-11-23).

**Logging.** One `alpaca.http` line per call that returns or raises: method, path template, order
id (own field), status or error type, elapsed ms, `X-Request-ID`; Alpaca's `message` for a non-2xx.
Never headers or keys. A call cancelled in flight (the engine's I/O budget, a shutdown) writes
none until WP 1.6d (PLAN 1.6 row).
The request id is also in every `BrokerError` message raised from a response, so an operator can
quote it to Alpaca.

### Why a connect failure proves "not sent" (httpx/httpcore verification)

`RequestNotSent` (recorded `not_submitted`, no resolver, no incident) rests on httpcore/httpx
internals. Verified 2026-11-22 by reading the installed sources: **httpx 0.28.1, httpcore 1.0.9,
anyio 4.16.0** (h11 0.16.0 transitive). The mapping `ConnectError`, `ConnectTimeout` and
`PoolTimeout` to `RequestNotSent` is safe only if these five facts hold:

1. **The connection is fully established before the first request byte.** In
   `httpcore/_async/connection.py` `AsyncHTTPConnection.handle_async_request`, `_connect()`
   (`connect_tcp`, then `start_tls` for https) returns before the request reaches the HTTP/1.1
   connection. `ConnectError` / `ConnectTimeout` are raised only by the exception maps in
   `httpcore/_backends/anyio.py` (`connect_tcp`, `connect_unix_socket`, `start_tls`); there is no
   other raise site (`connection.py` only catches them, in the retry loop that `retries=0`
   disables).
2. **A write-phase failure closes the connection.** `httpcore/_async/http11.py`
   `handle_async_request`: `WriteError` is suppressed and the response is read (so the outcome
   is a read error or a status, never a connect error); any other exception, `CancelledError`
   included, runs `_response_closed` under `AsyncShieldCancellation` in the `except BaseException`
   path, which closes the stream. A cancelled write is cut off, not finished.
3. **`PoolTimeout` only around the wait for a connection.** `httpcore/_synchronization.py`
   `AsyncEvent.wait` is its only async raise site, reached from
   `AsyncPoolRequest.wait_for_connection` (`connection_pool.py`) before any connection handles the
   request. The pool's retry loop re-queues only on `ConnectionNotAvailable`, which HTTP/1.1
   raises only in a state check before any write (`http11.py`).
4. **httpx maps the three 1:1.** `httpx/_transports/default.py` `_load_httpcore_exceptions` /
   `map_httpcore_exceptions` map `httpcore.ConnectError`, `ConnectTimeout` and `PoolTimeout` to the
   `httpx` exceptions of the same name. None of the three has a subclass and httpx raises them
   nowhere else.
5. **(C1) `start_tls` closes the socket only on `Exception`.** `httpcore/_backends/anyio.py`
   `AnyIOStream.start_tls`: `except Exception: await self.aclose(); raise`. A `CancelledError`
   during the TLS handshake skips the close and would leak the socket. Nothing is written by
   then, so the request is still "not sent", but the connection must not leak.

**Conditions** the transport keeps and the argument needs (each pinned by a unit test,
`test_production_transports_ignore_env_and_never_retry`):
- **no proxy**: the proxy modules call `start_tls` after bytes were written. A plain
  `httpcore.AsyncConnectionPool` is built (`proxy` unset) and the clients use `trust_env=False`;
  the test checks the pool's type is exactly `AsyncConnectionPool` (`AsyncHTTPProxy` subclasses
  it) and that no proxy transport is mounted;
- **HTTP/1.1 only**: `http2=False` (ALPN offers `http/1.1` only);
- **`retries=0`** on the transport;
- **the anyio backend**: `ClosingBackend` delegates to `httpcore.AnyIOBackend`.

**The closing backend (C1).** `ClosingBackend` wraps httpcore's anyio backend and closes the
inner stream when `start_tls` ends in a `BaseException` (cancellation), then re-raises. httpx has
no public hook for a network backend, so `make_transport` sets the **private**
`transport._pool._network_backend` after construction (httpcore reads it per new connection). A
loopback test (a server that never answers the handshake) pins the closing; removing the line
makes it fail.

**Re-check all of this on any httpx, httpcore or anyio upgrade:** facts 1-5, the four conditions
and the private `_pool._network_backend` hook (and the private pool attributes
`test_production_transports_ignore_env_and_never_retry` reads). The verified versions are
the ones `uv.lock` resolves (exact pins there). `pyproject.toml` declares ranges
(`httpx>=0.28.1,<0.29`, `httpcore>=1.0.9,<1.1`, `anyio>=4.16,<5`) that allow upgrades, for
example a `uv lock --upgrade`; any version change in the lock requires re-running this
verification. `test_the_transport_runs_on_the_versions_the_not_sent_proof_was_verified_on`
(`tests/unit/broker/test_alpaca_http.py`, `VERIFIED_TRANSPORT_VERSIONS`, also pins h11) fails on
any such change until the pin is moved, which is done only after this verification. If a fact no longer holds, `RequestNotSent` is unsafe: stop and take it to
the owner.

## Market data (2026-10-11)

- **REST bars** (`raw_data=True`): `{symbol: [{"t": "…Z", "o", "h", "l", "c", "v", "n", "vw"}]}`, prices as floats. Fine for bars, which are indicator input.
- **The stream** sends msgpack payloads: `"S"` (symbol) and `"t"` as a msgpack `Timestamp`, not a string.
- **The feed is sparse.** Over 2 days:

  | Symbol | 1m bars missing |
  |---|---|
  | BTC/USD | 475 of 2,880 (16 %) |
  | SOL/USD | 1,454 of 2,880 (**50 %**) |

  Over an 8.5-day backfill there were 11,009 BTC, 5,303 ETH and 6,893 SOL bars (of ~12,240 minutes). Some bars have zero trades (`n: 0`). BTC volume per minute is tiny (e.g. 0.00015 BTC).
- **Density depends strongly on time.** Share of minutes with a 1m bar, from the 8.5-day backfill (Fri 2026-10-02 22:00 → Sun 2026-10-11 10:00 UTC):

  | Symbol | Weekday | Weekend | Best UTC hour |
  |---|---|---|---|
  | BTC/USD | 98 % | 78 % | 15:00 (96 %) |
  | ETH/USD | 55 % | 26 % | 14:00 (68 %) |
  | SOL/USD | 67 % | 40 % | 14:00 (80 %) |

  The peak is 13:00–16:00 UTC (US open overlapping Europe), the low 20:00–23:00 UTC. Weekends are roughly half as dense. BTC is fine on weekdays; ETH stays thin even at peak. The two-day figures above include a weekend, so they understate weekday quality.
- **Quotes only arrive on book changes.** In a 65 s live probe: BTC 0–4 quotes, ETH 5, SOL 0.
- **Consequences:**
  - "No data for 60 s" can't tell a broken feed from a quiet market. The feed uses split health instead (PLAN D11).
  - **Verify before finalising:** stream bars are only pending until one REST fetch per 5m window (all symbols) confirms the window. Missed minutes are merged in before higher timeframes are built; empty buckets become flat bars (G5).
  - **Strategy concern:** thin volume makes `vol_z`-style signals on this venue suspect. Check in WP 1.7 before relying on ETH/SOL.
- **Library internals:** the live stream runs via `CryptoDataStream._run_forever()` (private; the public `run()` starts its own loop), and `connected()` reads the private `_running` flag. Re-check both on `alpaca-py` upgrades.

## Not verified yet

- **`CFEE`/`FEE` posting time, paper and live.** Paper posted a `CFEE` per leg (re-query 2026-10-14), but when a record first becomes visible is unverified. Check live behaviour on the first live fills (Phase 5).
- **Unexplained paper cash change of -19.84 USD** between the 2026-10-12 18:03Z and 2026-10-14 16:37Z snapshots, with the activities reconciling the earlier balance (re-query section). Re-check, and ask Alpaca if it persists.
- **Exact cash rounding rule** (truncate vs round) at sizes where the difference exceeds one cent. One $10 sample can't settle it.
- **Stop-limit behaviour** on crypto (trigger price source, gaps). Paper-test in WP 1.5 if a broker-side backup stop is wanted.
- **Order placement for WP 1.6 (order resolver, WP 1.5d-2, 2026-11-11).** Exits are now a market sell of an exact qty through `submit_order`, not `close_position`. 1.6 must verify, per traded symbol, that an exact-qty sell of the free holding closes cleanly (the 2026-10-12 probe was one BTC sample), confirm the `qty_available` mapping in `AlpacaBroker.positions` (already read from the API), and document what Alpaca does on a duplicate `client_order_id` at submit (rejection text, whether the existing order is returned). 1.6 also implements `get_order_by_client_id` (today `repair link` fails with `OrderMethodsUnavailable`) and must check the lookup is visible for `order_visibility_allowance_s` (60 s) after a submit. *Added 2026-11-11 (PR #35 review, m2):* when mapping the lookup to `BrokerOrderView`, also map order type, limit price and time in force (the match check needs them), and verify the order's `qty` comes back exactly as submitted for market and limit orders on each traded symbol: the match is exact, so any normalisation must be found here, not at the first lost response (PLAN 1.6 row).
  *Partly answered 2026-11-12 (WP 1.6a probes, section above): exact-qty sells closed cleanly on BTC, ETH and SOL, qty is echoed unchanged for the entry and exit shapes, a duplicate client id is a 422 without the existing order, the lookup saw each order within 0.3 s. Still open: the `qty_available` mapping under a resting sell, and behaviour beyond one sample per symbol.*
- **Live availability for German residents** (SPEC §2, open risk). Unchanged; not checkable from a paper account.
