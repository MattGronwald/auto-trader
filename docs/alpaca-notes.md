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

**Consequences:**
1. **`SimBroker` currently charges all fees in USD.** That is right for sells but wrong for buys, where the real position is smaller than the ordered qty. Owner decision D10 (2026-10-11 review) selects coin-denominated buy fees and dust handling; PLAN 1.R2 implements it. The simulator has not yet been changed.
2. **Reconciliation (WP 1.5):** the empirical step-1 probe below shows the buy fee deducted from coin **at fill time**; only the activity record is posted later. Gross fill qty and net broker position therefore differ immediately. Explain that difference using fee denomination and bounded rounding; an unexplained discrepancy still pauses new entries. Do not hide discrepancies by unconditionally overwriting local qty.
3. **Journal (WP 1.5 basic accounting; WP 3.1 activity reconciliation):** the posted fee record is not available at fill time. Record an **estimated** fee from `fee_schedule()` at fill time, then reconcile it with the posted `CFEE` amount once available, retaining its asset denomination and avoiding double counting.
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

- **`CFEE` posting and the sell-side fee:** fee check step 2.
- **Stop-limit behaviour** on crypto (trigger price source, gaps). Paper-test in WP 1.5 if a broker-side backup stop is wanted.
- **Live availability for German residents** (SPEC §2, open risk). Unchanged; not checkable from a paper account.
