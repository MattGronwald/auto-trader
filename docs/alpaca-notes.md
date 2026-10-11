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
1. **`SimBroker` charges all fees in USD.** That is right for sells but wrong for buys, where the real position is smaller than the ordered qty. → Decision needed (see below).
2. **Reconciliation (WP 1.5):** a fresh fill and the broker position can disagree until the end-of-day fee posting. Reconcile against the broker-reported qty and adopt it (G10 resolution). A mismatch equal to the expected fee must not trigger a pause.
3. **Journal (WP 3.1):** the fee of a fill is not known at fill time. Record an **estimated** fee from `fee_schedule()` at fill time, then replace it with the posted `CFEE` amount once available.
4. **Exits sell the actual position qty**, not the ordered entry qty, or they would hit "insufficient qty" after the fee posting.

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
- `price_increment` isn't in `AssetRules` yet. Add it when limit-price rounding is needed (Phase 1).
- **The API returns these as strings.** The `alpaca-py` models turn them into floats, so `AlpacaBroker` uses `raw_data=True` and reads them as exact `Decimal`s.

## Account

- Paper account: `status` ACTIVE, `crypto_status` ACTIVE, currency USD, $100,000 paper cash.
- No margin and no shorting for crypto (`marginable`/`shortable` false). Buys are checked against `non_marginable_buying_power`.

## Not verified yet

- **Position symbol format.** Alpaca may report crypto positions as `BTCUSD` rather than `BTC/USD`. With no positions there was nothing to check. `AlpacaBroker` accepts both and normalises to `BASE/QUOTE`; confirm with the first paper fill.
- **Fee deduction in practice.** Whether the position qty shrinks at the fill or at the end-of-day posting, and what `CFEE` looks like. Needs one tiny paper order, checked again the next day (see below).
- **Stop-limit behaviour** on crypto (trigger price source, gaps). Paper-test in WP 1.5 if a broker-side backup stop is wanted.
- **Live availability for German residents** (SPEC §2, open risk). Unchanged; not checkable from a paper account.

## Proposed empirical check (needs owner approval)

1. Buy ~$10 of BTC/USD on **paper** at market and record the fill qty, fill price, position qty and `qty_available`.
2. Next day: record the position qty again plus the `CFEE` activity.
3. Sell the position. Record the USD fee and the end-of-day posting.

This settles the position-symbol and fee-timing questions above, and gives recorded fixtures for WP 1.6.
