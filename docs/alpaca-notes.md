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

**Consequences:**
1. **`SimBroker` currently charges all fees in USD.** That is right for sells but wrong for buys, where the real position is smaller than the ordered qty. Owner decision D10 (2026-10-11 review) selects coin-denominated buy fees and dust handling; PLAN 1.R2 implements it. The simulator has not yet been changed. **2026-10-12 (WP 1.R1b):** `FeeSchedule.buy_fee_asset` now states the denomination: `AlpacaBroker` reports `base`, and SimBroker reports `quote` and refuses `base` until 1.R2. Risk sizing and expectancy use the matching cash-flow equation.
2. **Reconciliation (WP 1.5):** the empirical step-1 probe below shows the buy fee deducted from coin **at fill time**; the sell fee likewise comes out of the USD proceeds at fill (step 2). No fee activity record was observed in the recorded paper queries. Gross fill qty and net broker position therefore differ immediately. Explain that difference using fee denomination and bounded rounding; an unexplained discrepancy still pauses new entries. Do not hide discrepancies by unconditionally overwriting local qty.
3. **Journal (WP 1.5 basic accounting; WP 3.1 activity reconciliation):** the posted fee record is not available at fill time. Record an **estimated** fee from `fee_schedule()` at fill time, then reconcile it with the posted `CFEE` amount once available, retaining its asset denomination and avoiding double counting.
   **2026-10-12 (corrected after the PR #16 post-merge review):** no fee activity has been observed on paper so far, so accounting must not block on one. Activity reconciliation stays in scope for both paper and live, and must not double count against the coin/cash deductions already applied.
   - **A gross-minus-net residual is not the fee.** The residual is gross fill qty minus net received qty (buys), or gross fill notional minus cash credited (sells). It mixes fee and rounding. In the step-2 sell, the residual is 0.0305 USD; at 25 bps the fee would be 0.0243, and the other 0.0063 would be rounding.
   - **It is only per-order in isolated probes.** Account cash and position qty are aggregates, so another fill between snapshots contaminates the delta.
   - **So:** record fee and rounding as estimates, with provenance (schedule rate, which snapshot) and bounds. Finalise fees/P&L only after fill-linked reconciliation of all relevant cash and position changes.
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
- `min_order_size` differs per asset and looks like **~1 USD at current prices**. This is inferred, not documented, so it likely moves with price. Fetch it at sizing time; don't cache it for long.
- **No minimum notional is published.** There is a **maximum of $200k notional per order**, irrelevant at PoC size. `AssetRules.min_notional` is therefore `0` for Alpaca.
- `price_increment` is mapped into `AssetRules.price_increment` (WP 1.3) and used to round the marketable entry limit up.
- **The API returns these as strings.** The `alpaca-py` models turn them into floats, so `AlpacaBroker` uses `raw_data=True` and reads them as exact `Decimal`s.

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
- **No `CFEE`/`FEE` activity observed yet:** none ~32 h after the buy, none right after the sell. A later posting for either leg isn't ruled out, so accounting must not block on one (see G10 consequence 3). *(Bounded 2026-10-12 after the PR #16 post-merge review; the first version claimed paper never posts these.)*

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

- **`CFEE`/`FEE` posting, paper and live.** None observed on paper as of 2026-10-12. Re-query the step-2 sell after an end-of-day (read-only); on live, check the first fills (Phase 5).
- **Exact cash rounding rule** (truncate vs round) at sizes where the difference exceeds one cent. One $10 sample can't settle it.
- **Stop-limit behaviour** on crypto (trigger price source, gaps). Paper-test in WP 1.5 if a broker-side backup stop is wanted.
- **Live availability for German residents** (SPEC §2, open risk). Unchanged; not checkable from a paper account.
