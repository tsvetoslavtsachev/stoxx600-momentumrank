# P7a-2 — STOXX600 stock citizen: foreign-exchange ticker risk investigation

> **STATUS: research handoff.** Committed to the STOXX600 dashboard repo because the
> price-archive monorepo is not reachable from this session. Carry this into the
> monorepo session to write the actual P7a-2 mandate + build (see "Environment blocker").

> Pre-build research (the KEY risk that differs from SP500/P7a). Grounded in the
> live STOXX600 dashboard universe (`data.json`, 597 current members, iShares EXSA
> source) + `fetch_data.py` / `momentum_core.py` exchange logic. **Not yet a mandate**
> — the P7a mandate template, `collectors/price/` backend, and `project_init22_*`
> memory cards are NOT present in this checkout (see "Environment blocker" at end).

## 0. Universe facts (verify of the ~591 estimate)
- **597 current members** today (`data_meta.json count: 597`, fresh 596 / stale 1 /
  missing 3). Source = iShares EXSA holdings CSV (current-members-only → inherently
  **survivorship-biased**, exactly the P7a `family:"stock"` situation).
- Sectors already normalized to the 11 GICS-style buckets (Industrials 133, Financial
  Services 126, Consumer Cyclical 58, …) → reuse for the `sector` category dimension.

## 1. yfinance suffix / exchange (the foreign-exchange core)
- **Every symbol carries a Yahoo exchange suffix** — ZERO bare tickers. Distribution:
  `.L 124, .DE 70, .PA 69, .SW 60, .ST 51, .MI 40, .AS 35, .MC 28, .OL 23, .CO 23,
   .HE 19, .WA 18, .BR 17, .VI 9, .IR 6, .LS 5` (16 exchanges).
- The dashboard already owns a battle-tested `EXCHANGE_SUFFIX` map + `COUNTRY_SUFFIX_FALLBACK`
  + `TICKER_OVERRIDES_BY_EXCHANGE` (e.g. Roche `ROP`→`ROG`, BT `BT-A`) in `fetch_data.py`.
  **Reuse this as the authoritative ticker→yahoo resolver; do not re-derive.**

## 2. CURRENCY — 8 currencies, and suffix does NOT imply currency
- `EUR 298, GBP 122, CHF 60, SEK 51, NOK 23, DKK 23, PLN 18, USD 2`.
- **`.L` is not always GBP**: `IHG.L` and `CPG.L` are quoted in **USD** on LSE.
  → currency MUST be stored per-series (from iShares), never inferred from suffix.
- **London pence (GBX) gotcha**: `.L`/GBP names quote in **pence**, not pounds
  (`HSBA.L=1445.4`, `RR.L=1432.0`, `IGG.L=1858.0`). The dashboard leaves them raw
  because momentum is ratio-based (scale cancels). **A price archive for TA cannot** —
  the basis (GBp vs GBP) must be recorded explicitly per series, or cross-asset/-currency
  charts are off by 100×. → PIN a `quote_basis`/`currency` field; decide GBX→GBP policy.

## 3. series_id normalization (dots + dashes)
- Symbols contain exactly ONE dot (the suffix) and, for ~43 Nordic/UK B-share classes,
  a dash: `SWED-A.ST, INVE-B.ST, BT-A.L, MAERSK-B.CO, NOVO-B.CO, …`. No other special chars.
- Both `.` and `-` are unsafe in a `px_<id>` series_id. → PIN normalization:
  `lower(symbol).replace('.','_').replace('-','_')`, e.g. `BT-A.L → px_bt_a_l`,
  `SAN.MC → px_san_mc`. **Keep the exchange suffix in the id** (see §4).

## 4. COLLISION analysis vs 132 ETF + 503 SP500 (the cardinal risk)
- SP500 + ETF series are **bare** tickers (`AAPL`, `SPY`). STOXX series **all** carry an
  exchange suffix. → If series_id **retains the suffix**, cross-archive collision is
  **structurally zero** (`px_san_mc` can never equal a bare US `px_san`). This is the
  decisive reason to keep the suffix in the id (also disambiguates dual-listings).
- **Within STOXX**, stripping the suffix WOULD collide: base-ticker dups `SAN`×2
  (Sanofi `SAN.PA` vs Santander `SAN.MC`), `UNI`×2, `BOL`×2. → suffix-retention also
  required intra-archive. **Verify gate: 0 collisions across the ~1226-series union**
  (132 + 503 + ~591) after normalization.

## 5. dual-basis + split_factor (verify-gate inputs)
- `momentum_core` downloads with **`auto_adjust=True`** → split+dividend adjusted Close
  only; no raw, no split_factor today. P7a's dual-basis (raw + adjusted) + `split_factor`
  must be produced for STOXX too. EU splits do occur → the P8 split-heal mechanic must be
  checked against EU corporate actions, not just US (flagged out-of-scope here, but noted).
- Good test fixtures for the gate: a foreign-exchange name (e.g. `SAP.DE` / `MC.PA`) +
  a known EU split (e.g. a recent Nordic/DE split) to confirm `split_factor` ≠ 1 path.

## 6. Survivorship flag (mirror P7a)
- iShares = current holdings only → every STOXX series gets the same machine-readable
  flag as SP500 stocks: `backtest_valid: false` + `members_basis: current-only`.
  Verify gate: flag present & machine-readable on EVERY STOXX series.

## PINNED DECISIONS (proposed, for sign-off)
1. Universe source = iShares EXSA holdings CSV (reuse dashboard Phase-0); ~597 current members.
2. `family: "stock"`; survivorship `backtest_valid:false` + current-members-only.
3. series_id = `px_` + `symbol.lower()` with `.`→`_`, `-`→`_`, **suffix retained** → 0 collisions.
4. currency stored per-series from iShares (NOT inferred from suffix); record quote basis;
   decide explicit GBX→GBP normalization for `.L` GBP names.
5. sector = existing 11-bucket normalization.
6. per-family depth via existing `history_period_stock` (~6y).
7. REUSE register_catalog `entry()` / fetch / push + dual-basis + split_factor exactly as P7a.

## Environment blocker (must resolve before build)
This checkout (`/home/user/stoxx600-momentumrank`) is the **standalone STOXX600 dashboard**,
NOT the price-archive monorepo. Absent here: `collectors/price/` (config.yaml,
register_catalog.py, fetch_prices.py, to_datacore.py, tests/test_stock.py),
`initiatives/22-repo-revision-program/` (HANDOFF.md + P7a mandate template), and the
`project_init22_*` Recall cards (Recall KB holds only unrelated finance/AI research).
GitHub scope is limited to `tsvetoslavtsachev/stoxx600-momentumrank`.
→ Cannot mirror the P7a mandate, reuse its backend, or write into `initiatives/` from here.
